//! The tail cursors the multiplexed wait has already decoded.
//!
//! A client re-issues its wait after every answer with all its tails'
//! cursors, and every tail but the answered ones carries the cursor it
//! carried before. Decoding one derives the cursor's MAC key and checks its
//! MAC, the largest part of a re-issued tail's CPU, for the same result
//! each time: `ReadCursor::decode` is a pure function of the token, the
//! project, the key, the incarnation and the routing key. So a decoded
//! position is remembered under exactly those inputs, with the key named by
//! its fingerprint: the memo is consulted only after the request's key was
//! verified against the descriptor's fingerprint, so an equal fingerprint
//! is the same key, and the memo holds no key material. A token that does
//! not decode is never remembered. A full memo starts over.
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use super::super::read_cursor::CursorBinding;
use crate::application::read::{ReadFailure, ReadStart};
use crate::tenant::ProjectId;

/// At most this many decoded cursors are held, a few hundred bytes each:
/// twenty tails for each of a few hundred tenants on a cell.
const MEMO_CAP: usize = 8_192;

/// Everything a decode depends on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Decoded {
    pub(super) project: ProjectId,
    pub(super) epoch: [u8; 16],
    /// The verified key's fingerprint, standing for the key.
    pub(super) fingerprint: String,
    pub(super) routing_key: String,
    pub(super) token: String,
}

/// The decoded positions, by what decoded them.
#[derive(Default)]
pub(super) struct CursorMemo(Mutex<HashMap<Decoded, ReadStart>>);

impl CursorMemo {
    /// The position `decoded` names: remembered, else `decode`'s, which is
    /// remembered when it decodes.
    pub(super) fn remembered<E>(
        &self,
        decoded: Decoded,
        decode: impl FnOnce() -> Result<ReadStart, E>,
    ) -> Result<ReadStart, E> {
        if let Some(start) = self.held().get(&decoded).copied() {
            return Ok(start);
        }
        let start = decode()?;
        let mut held = self.held();
        if held.len() >= MEMO_CAP {
            held.clear();
        }
        held.insert(decoded, start);
        Ok(start)
    }

    /// A poisoned memo holds only positions that decoded: nothing to undo.
    fn held(&self) -> std::sync::MutexGuard<'_, HashMap<Decoded, ReadStart>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.held().len()
    }
}

static MEMO: OnceLock<CursorMemo> = OnceLock::new();

/// `binding.start(cursor)`, remembered for a token under `decoded`'s other
/// inputs. `beginning`, `now` and no cursor decode nothing.
pub(super) fn start(
    binding: &CursorBinding<'_>,
    cursor: Option<&str>,
    decoded: impl FnOnce(String) -> Decoded,
) -> Result<ReadStart, ReadFailure> {
    match cursor {
        None | Some("" | "beginning" | "now") => binding.start(cursor),
        Some(token) => MEMO
            .get_or_init(CursorMemo::default)
            .remembered(decoded(token.to_owned()), || binding.start(Some(token))),
    }
}

#[cfg(test)]
mod tests {
    use super::{CursorMemo, Decoded, MEMO_CAP};
    use crate::application::read::{ReadPosition, ReadStart};
    use std::cell::Cell;

    fn decoded(token: &str) -> Decoded {
        Decoded {
            project: crate::tenant::ProjectId::new("proj-a").unwrap(),
            epoch: [3; 16],
            fingerprint: "f1".into(),
            routing_key: String::new(),
            token: token.into(),
        }
    }

    fn at(after: u64) -> ReadStart {
        ReadStart::Position(ReadPosition { segment: 0, after })
    }

    fn offset(start: ReadStart) -> u64 {
        match start {
            ReadStart::Position(p) => p.after,
            other => panic!("a position: {other:?}"),
        }
    }

    /// A re-issued cursor decodes once; any other input decodes again.
    #[test]
    fn an_unchanged_cursor_decodes_once_and_any_other_input_again() {
        let memo = CursorMemo::default();
        let decodes = Cell::new(0u32);
        let decode = |after: u64| {
            decodes.set(decodes.get() + 1);
            Ok::<_, ()>(at(after))
        };
        assert_eq!(
            offset(memo.remembered(decoded("t1"), || decode(7)).unwrap()),
            7
        );
        assert_eq!(
            offset(memo.remembered(decoded("t1"), || decode(9)).unwrap()),
            7
        );
        assert_eq!(decodes.get(), 1, "the second wait reused the first decode");
        let others = [
            Decoded {
                token: "t2".into(),
                ..decoded("t1")
            },
            Decoded {
                fingerprint: "f2".into(),
                ..decoded("t1")
            },
            Decoded {
                routing_key: "lane".into(),
                ..decoded("t1")
            },
            Decoded {
                epoch: [4; 16],
                ..decoded("t1")
            },
            Decoded {
                project: crate::tenant::ProjectId::new("proj-b").unwrap(),
                ..decoded("t1")
            },
        ];
        for (n, other) in others.into_iter().enumerate() {
            let after = 10 + n as u64;
            assert_eq!(
                offset(memo.remembered(other, || decode(after)).unwrap()),
                after
            );
        }
        assert_eq!((decodes.get(), memo.len()), (6, 6));
    }

    /// A token that does not decode is refused every time and never held.
    #[test]
    fn a_cursor_that_does_not_decode_is_never_remembered() {
        let memo = CursorMemo::default();
        for _ in 0..2 {
            assert!(matches!(
                memo.remembered(decoded("bad"), || Err::<ReadStart, _>("invalid")),
                Err("invalid")
            ));
        }
        assert_eq!(memo.len(), 0);
        assert_eq!(
            offset(
                memo.remembered(decoded("bad"), || Ok::<_, ()>(at(1)))
                    .unwrap()
            ),
            1
        );
    }

    /// A full memo starts over rather than growing past its cap.
    #[test]
    fn a_full_memo_starts_over() {
        let memo = CursorMemo::default();
        for n in 0..MEMO_CAP {
            memo.remembered(decoded(&n.to_string()), || Ok::<_, ()>(at(0)))
                .unwrap();
        }
        assert_eq!(memo.len(), MEMO_CAP);
        memo.remembered(decoded("one more"), || Ok::<_, ()>(at(0)))
            .unwrap();
        assert_eq!(memo.len(), 1);
    }
}
