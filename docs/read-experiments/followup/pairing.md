# Explicit pairing for the earlier campaign

The previous summary displayed block arrays in filesystem enumeration order, while its paired bootstrap used sorted numeric block IDs from the raw filenames. Array position therefore did not identify a pair. The repair exposes the actual pair ID rather than changing the observations or pairing by the displayed order.

The local extraction joins each raw filename to its successful execution receipt and records its SHA-256, base revision/tree, instrumented-source description, native binary SHA-256 and block percentiles. `identified_pairs.py summarize` uses those explicit IDs with the original bootstrap seed, resample count and nearest-rank quantiles. `verify_pairing.py` reproduced all 120 published ratio/interval triplets checked to 1e-12 tolerance. There were no CI corrections.

For the review's mimalloc offered-history example at 500/s, the actual pairs are:

| Block ID | Original median (µs) | Source median (µs) |
| --- | ---: | ---: |
| 1 | 1,805 | 2,297 |
| 2 | 1,934 | 2,628 |
| 3 | 1,568 | 2,225 |

These yield a geometric mean ratio of 1.3487937525558578 and the originally reported 95% interval [1.2725761772853186, 1.4190051020408163].

This makes the pairing and arithmetic reviewable. It does not turn the author's native measurements into an independent reproduction. Raw individual samples, executable binaries and the complete archive remain under the separate evidence-upload hold; hashes bind those local artifacts without pretending that recipients have independently verified them.
