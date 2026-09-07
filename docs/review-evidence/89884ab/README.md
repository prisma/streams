# Review follow-up evidence at 89884ab

The seven follow-up engineering findings are implemented in 18 separate source commits and ready for review. Source branch `slate` was pushed at [89884ab114b22929aa93707361e9e8d52c762bc2](https://github.com/prisma/streams/commit/89884ab114b22929aa93707361e9e8d52c762bc2), tree `dc26e246b25fb6275374d3d56088c165901d19c9`. This evidence-only commit adds the report and artifacts after that source freeze; it is not represented as the runtime-tested revision.

- [Full change and verification report](report.md)
- [Machine-readable final results](results.json)
- [Performance comparison](performance.md), [raw summary](performance.json) and [process RSS](performance-rss.json)
- [Portable verification bundle](verification-bundle.zip) (47,540,234 bytes)
- [Actual delivered-ZIP verification transcript](verification.log) and [SHA-256 checksums](SHA256SUMS)
- [18 follow-up commits](followup-commits.txt) and [111 original plus follow-up source commits](all-commits.txt)
- [Successful final CI](https://github.com/prisma/streams/actions/runs/34050181550) and [workflow lint](https://github.com/prisma/streams/actions/runs/34050181553)

Final successful local counts: 885 library tests (884 general plus one isolated capacity), 76 binary target tests, zero rustdoc tests, 33 SDK tests, and 332 protocol tests with six expected reserved subscription API skips. Earlier failed attempts, including an intermittent final-source local timing-test failure followed by an unchanged successful full rerun, remain in the bundle and report. CI ran nine jobs successfully; the scheduled-only noisy campaign was skipped.

Download and extract the ZIP, then run `python3 verify-bundle.py /path/to/extracted-bundle`. The actual verification checked 346 file hashes, reconstructed the Git tree/commit binding and verified four clean-source execution receipts. The separate verifier self-test passed ten negative controls. Hash consistency is not an independent execution signature.

Performance acceptance remains pending: the five paired rounds show median and tail regressions for hot filtered reads and many-record replay, while measured payload copies remain eliminated. Independent crypto/historical-ciphertext/rollout acceptance and deployed Prisma Compute/Tigris/noisy-neighbor/cost campaigns also remain with the designated reviewer and authorized owners described in the report. No reviewer sign-off, deployment approval or performance waiver is asserted.
