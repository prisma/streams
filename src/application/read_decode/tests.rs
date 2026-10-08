use super::*;
use crate::application::read::Watermarks;
use crate::application::read_retention_probe::Probe;
use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::{CheckedPage, PageCipher, PageLane};
use bytes::Bytes;

const RECORD: usize = 2048;
const PER_PAGE: u8 = 8;
const RECORDS: u8 = 64;

fn page() -> ReadPage {
    ReadPage {
        recs: PlainBatch::default(),
        watermarks: Watermarks {
            durable: 64,
            applied: 64,
        },
        last: None,
        end: 64,
        completed: true,
    }
}

/// Record `n`'s payload: 2 KiB of its own byte on even pages, which zstd
/// shrinks (version 7), and 2 KiB of noise on odd pages, which it does not
/// (version 6).
fn payload(n: u8) -> Vec<u8> {
    if (n / PER_PAGE).is_multiple_of(2) {
        return vec![n; RECORD];
    }
    let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ u64::from(n);
    (0..RECORD)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A segment's 64 records in eight pages of eight, sealed under the
/// unkeyed lane of one stream key and admitted as a scan admits them.
struct Pages {
    key: StreamKey,
    epoch: [u8; 16],
    segment: [u8; 16],
    pages: Vec<CheckedPage>,
}

impl Pages {
    fn new() -> Self {
        let (key, epoch, segment) = (StreamKey([7; 32]), [8; 16], [9; 16]);
        let cipher = PageCipher::new(&derive_subkey(&key, &epoch, "", 0), &segment);
        let lane = PageLane {
            key_version: 0,
            routing_key: "",
        };
        let payloads: Vec<Vec<u8>> = (0..RECORDS).map(payload).collect();
        let pages = payloads
            .chunks(usize::from(PER_PAGE))
            .zip((0..).step_by(usize::from(PER_PAGE)))
            .map(|(records, first)| {
                let sealed = cipher
                    .seal(&lane, first, &crate::crypto_page::stamped(123, records))
                    .unwrap();
                CheckedPage::admit(Bytes::from(sealed.bytes), sealed.last).unwrap()
            })
            .collect();
        Self {
            key,
            epoch,
            segment,
            pages,
        }
    }

    /// The slices a scan of `[from, to)` returns.
    fn slices(&self, from: u64, to: u64) -> PageSlices {
        let mut slices = PageSlices::default();
        for page in &self.pages {
            if let Some(slice) = PageSlice::clip(page.clone(), from, to) {
                slices.push(slice);
            }
        }
        slices
    }

    fn decode(&self, slices: &PageSlices, budget: usize) -> (Result<bool, String>, ReadPage) {
        let mut out = page();
        let mut keys = ReadKeys::new(&self.key, &self.epoch, self.segment);
        let result = decode_frames_into(slices, &mut keys, &mut out, &mut PageBudget::new(budget));
        (result, out)
    }

    /// The same pages with the tag of page `index` broken.
    fn tampered(&self, index: usize) -> Self {
        let mut pages = self.pages.clone();
        let mut raw = pages[index].raw().to_vec();
        *raw.last_mut().unwrap() ^= 1;
        pages[index] = CheckedPage::admit(Bytes::from(raw), pages[index].last()).unwrap();
        Self {
            key: StreamKey(self.key.0),
            pages,
            ..*self
        }
    }
}

fn offsets(out: &ReadPage) -> Vec<u64> {
    out.recs.iter().map(|record| record.off).collect()
}

fn exact(out: &ReadPage) -> bool {
    out.recs
        .iter()
        .all(|record| u8::try_from(record.off).is_ok_and(|n| *record.payload == payload(n)))
}

/// Raw and compressed pages decode alike into one owner. A budget that ends
/// inside a page publishes exactly the records before it; a page that does
/// not authenticate fails the whole read and publishes nothing, but a page
/// the budget never reaches is never opened.
#[tokio::test]
async fn o2b_mixed_compressed_pages_preserve_failure_and_withholding_boundaries() {
    let fixture = Pages::new();
    let compressed: Vec<bool> = fixture
        .pages
        .iter()
        .map(CheckedPage::is_compressed)
        .collect();
    assert_eq!(
        compressed,
        [true, false, true, false, true, false, true, false]
    );
    let all = fixture.slices(0, 64);
    let probe = Probe::default();
    probe
        .scope(async {
            let (result, complete) = fixture.decode(&all, 128 << 10);
            assert_eq!(result, Ok(true));
            assert_eq!(offsets(&complete), (0..64).collect::<Vec<u64>>());
            assert!(exact(&complete));
            assert_eq!(complete.last, Some(63));
            assert_eq!(complete.recs.retained_capacity(), 128 << 10);
            let owner = complete
                .recs
                .contiguous()
                .expect("one owner for every page");
            assert_eq!(owner.len(), 128 << 10);
            drop((complete, owner));
            assert_eq!(probe.live(), 0);
            let (result, partial) = fixture.decode(&all, 5 << 10);
            assert_eq!(result, Ok(false));
            assert_eq!((offsets(&partial), partial.last), (vec![0, 1], Some(1)));
            assert_eq!(partial.recs.retained_capacity(), 2 * RECORD);
            drop(partial);
            let last_broken = fixture.tampered(7);
            let broken = last_broken.slices(0, 64);
            let (result, failed) = last_broken.decode(&broken, 128 << 10);
            assert!(
                result
                    .unwrap_err()
                    .contains("stored page [56, 63] did not open")
            );
            assert!(failed.recs.is_empty());
            assert_eq!(probe.live(), 0, "no tentative owner survives a failed page");
            let (result, withheld) = last_broken.decode(&broken, 5 << 10);
            assert_eq!(result, Ok(false), "a page past the budget is never opened");
            assert_eq!(offsets(&withheld), [0, 1]);
            let first_broken = fixture.tampered(0);
            let (result, first_broken) = first_broken.decode(&first_broken.slices(0, 64), 1);
            assert!(
                result.is_err(),
                "a page authenticates before its first record"
            );
            assert!(first_broken.recs.is_empty());
            // A budget spent exactly at a page edge never opens the next page.
            let second_broken = fixture.tampered(1);
            let slices = second_broken.slices(0, 64);
            let (result, filled) = second_broken.decode(&slices, 8 * RECORD);
            assert_eq!(
                result,
                Ok(false),
                "the page after a full budget is not opened"
            );
            assert_eq!((offsets(&filled), filled.last), ((0..8).collect(), Some(7)));
        })
        .await;
    assert_eq!(probe.live(), 0);
}

/// A read whose window starts and ends inside pages decodes exactly the
/// window: the records of the first page below its cursor are skipped,
/// those of the last page past its end are not served.
#[test]
fn a_window_inside_pages_decodes_exactly_its_records() {
    let fixture = Pages::new();
    let (result, out) = fixture.decode(&fixture.slices(5, 59), 128 << 10);
    assert_eq!(result, Ok(true));
    assert_eq!(offsets(&out), (5..59).collect::<Vec<u64>>());
    assert!(exact(&out));
    assert_eq!(out.last, Some(58));
}

/// A pager whose budget ends inside pages resumes at the last returned
/// offset plus one, inside the same page: every record comes back exactly
/// once, in order. A budget of one byte still returns one record per read,
/// so the smallest pager always makes progress.
#[test]
fn a_budget_cut_inside_a_page_resumes_at_the_next_record() {
    let fixture = Pages::new();
    for (budget, per_read) in [(5 << 10, 2), (3 * RECORD, 3), (1, 1)] {
        let (mut from, mut served, mut reads) = (0, Vec::new(), 0);
        while from < 64 {
            let (result, out) = fixture.decode(&fixture.slices(from, 64), budget);
            let done = result.unwrap();
            let got = offsets(&out);
            assert!(exact(&out));
            assert_eq!(
                got.first(),
                Some(&from),
                "budget {budget}: resumed at {from}"
            );
            assert_eq!(out.last, got.last().copied());
            assert!(got.len() == per_read || done, "budget {budget}: {got:?}");
            from = out.last.unwrap() + 1;
            served.extend(got);
            reads += 1;
        }
        assert_eq!(served, (0..64).collect::<Vec<u64>>(), "budget {budget}");
        assert_eq!(reads, 64_usize.div_ceil(per_read), "budget {budget}");
    }
}
