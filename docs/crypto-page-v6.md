# Stored pages (layout 5): format, keys and cryptographic acceptance

Since storage layout 5, every record the server stores, in the shard log and
in history, is inside a **page**: one append request's records of one
routing key and key version, cut at 64 KiB of body and 4,096 records,
compressed together with zstd level 1 when that pays, and encrypted once with
AES-256-GCM-SIV. History holds the shard log's pages byte for byte. Pages
never leave the server: `format=frames` reads re-encrypt each record as a
version 4 frame ([crypto-frame-v4.md](crypto-frame-v4.md)), and JSON and raw
reads return plaintext. The code is `src/crypto_page.rs` and
`src/crypto_page/`; the decisions behind it are the layout 5 spec (owner
decisions of 2026-10-07).

## Format

```text
[ver u8: 6 raw | 7 zstd-1][first u64][count varint][ts_ms i64]
[key_version u32][rk_len u16][routing key][nonce 12]
[ct_len u32][ciphertext || tag 16]

body, the plaintext (compressed when ver is 7):
  count x record length         varint
  count x timestamp delta (ms)  varint, to the previous record's timestamp;
                                the first record's is 0, so ts_ms is the
                                first record's timestamp
  payloads, concatenated
```

- Integers are big-endian. A varint is minimal LEB128 (seven bits per byte,
  low group first, no redundant zero group), so every page has exactly one
  byte string. `ct_len` counts the ciphertext and its tag.
- Row keys: `hash16 ‖ 'p' ‖ last offset` in the shard log,
  `route16 ‖ inc16 ‖ 'g' ‖ last offset` in history (`'p'` rows there are
  postings pages). A scan of `[from, to)` reads from the key of `from` to the
  key of `to - 1 + 4,096` (`page_scan_bound`).
- Limits: count 1 to 4,096; a body of more than one record is at most
  64 KiB; a single record is at most the 32 MiB record cap, so a
  single-record body is at most 32 MiB + 5 (a four-byte length and its zero
  delta). Version 7 only when the body is at least 256 B and zstd makes it
  smaller.
- Version bytes 6 and 7 continue the frame numbering (frames are 2 to 5), so
  a page is never read as a frame, nor a frame as a page.

## Keys, AAD and nonces

```text
subkey   = HKDF-SHA256(salt = stream epoch, ikm = stream key,
                       info = routing key ‖ 0x00 ‖ key version LE32)
page key = HKDF-SHA256(salt = segment identity (16 B), ikm = subkey,
                       info = "prisma-streams/page/v6/aes-256-gcm-siv")
AAD      = segment identity ‖ header bytes from ver through nonce
nonce    = 12 bytes from OsRng per page
```

- The subkey is the frames' (`derive_subkey`, unchanged). The page key has
  its own HKDF label, so no page key equals a frame key of the same subkey
  and segment (a test shows a frame key does not open a page). Versions 6
  and 7 share one page key; `ver` is in the AAD, so a raw page cannot be
  read as compressed or the reverse.
- The segment identity salts the key and leads the AAD, so a page moved to
  another segment does not open. Every clear header byte is authenticated:
  first offset, count, timestamp, key version, routing key and nonce.
  `ct_len` is outside the AAD, but AES-GCM-SIV binds the plaintext length and
  admission requires the exact length.
- One random nonce per page, drawn after producer deduplication. A repeated
  nonce under AES-GCM-SIV reveals only the equality of identical (AAD,
  plaintext) pairs, and the AAD carries the page's first offset. Fixed
  nonces exist only in the private `seal_with_nonce`, for golden vectors.

## What is checked, and when

- **Before decryption, without a key** (`CheckedPage::from_row`, `admit`):
  the row tag of the keyspace the prefix names, the key width and namespace,
  version 6 or 7, a minimal count varint in 1 to 4,096, no truncated field, a
  UTF-8 routing key, the exact length with nothing trailing, a full 16-byte
  tag, a ciphertext no longer than the body cap of its count plus the tag,
  and the key's last offset equal to `first + count - 1` without overflow.
  So the work an unauthenticated row can cause is bounded by its cap.
- **After authentication** (`PageCipher::open_within`): a version 7 body
  must be exactly one standard zstd frame that declares its content size, no
  checksum, dictionary or reserved bit, of at most the page's cap, at least
  256 B and larger than its compressed form; it is decoded in one pass into
  a buffer of exactly that size (no window or streaming buffer). The body
  then parses exactly (`parse_body`): both tables hold `count` minimal
  varints, no length passes the record cap, the first delta is 0, every
  timestamp stays within i64, and the lengths tile the payload bytes. Nothing
  is returned until all of it holds.
- A read that cannot use a single-record page whose record is surely past
  its remaining budget neither decrypts it (raw) nor inflates it
  (compressed).
- A history read checks that the pages it meets tile its window: an
  overlapping or a lost page fails the read as corruption instead of serving
  offsets twice or passing records off as consumed.
- Opened pages and records have no Debug form, so plaintext cannot reach a
  log through `{:?}`.

Formal coverage: KANI-017 (admission, counts up to 4,096 and past it) and
KANI-097 (the body parser) in `verification/manifest.json`; TLA-016 (the
absorbed boundary is always a page edge) and TLA-018 (a lost page is
refused, cursors inside pages).

## Compression and length

- A stored page's size shows how well its request compressed. CRIME and
  BREACH-style attacks need attacker-chosen text compressed together with
  someone else's secret, and an observer of the compressed size.
- One page holds one request of one producer, so two producers' data is never
  compressed together. A producer can still mix trust domains in one
  request, for example a gateway that batches events of many end users of one
  customer: attacker-chosen fields and another user's secret can then share
  one page.
- Who can see a page's size: the storage provider and anyone who can list
  the bucket (WAL object sizes: on a quiet shard one WAL object holds about
  one page, and the shard log has no SlateDB compression), and the customer
  through the stored-byte usage counters (`ownedStoredBytesNow` is exact to
  the byte).
- Guidance for customers: do not batch secrets together with
  attacker-controlled data into one append if the storage operator is
  outside your trust boundary. The SDK's README states it under "Security
  notes" (the owner's decision of 2026-10-08, L6).
- The wire does not compress: `format=frames` answers version 4 frames on
  every deployment, so no response length depends on how well a record
  compresses (FRAME_COMPRESS is not read since layout 5).
- Plaintext sizes stay hidden: record lengths are inside the ciphertext, and
  the header carries no plaintext length (billing reads row sizes).

## Operating envelope

- At most 2^32 page encryptions per page key (segment, routing key, key
  version), as for frames: one encryption is one page. Batched producers
  encrypt about 60 times less often than with frames; a lane of single-record
  requests at the 5,000 records/s product limit reaches 2^32 pages in about
  10 days per segment, as it did with frames. Nothing counts invocations:
  rotate the key version or move to a new segment well before the bound.
- At most 64 KiB of body per multi-record page and 32 MiB + 5 per
  single-record page.

## Golden vectors and independent checks

`src/crypto_page/golden_tests.rs` pins the page key, a raw and a compressed
page under a fixed nonce, both row keys, and a stamped page: 130 records of
130 bytes under a key derived from a stream key (UTF-8 routing key, key
version 0x01020304, negative first timestamp, deltas of one to four varint
bytes, a two-byte count). The raw pages and the keys were reproduced byte for
byte by an independent implementation from this format text (stdlib
HMAC-HKDF and OpenSSL 3's AES-256-GCM-SIV), and the compressed pages were
authenticated and zstd-CLI-decoded to the format's body there.

## Acceptance

The owner accepted the page format's cryptography on 2026-10-07 and approved
three additions on the same day: the delta base fixed as delta to the
previous record, a seal API that takes a timestamp per record, and a golden
vector with non-zero deltas. The side-channel statements above (the wire
never compresses; one producer per page is not one trust domain per page)
are those of the layout 5 crypto review of 2026-10-08 (F2, F3), recorded
here with the acceptance. Primary references: RFC 8452 (AES-GCM-SIV) and
RFC 5869 (HKDF).
