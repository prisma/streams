//! The seal write path's building blocks (layout 6, DESIGN-6A §2.2-§2.4):
//! the segment object a seal writes (`object`) and its CRC-32C
//! (`crc32c`), the seal directory's rows and in-memory index (`directory`),
//! the pending pages' merge operator and codecs (`pending`), and the
//! memtable trigger in SlateDB's units (`estimate`) and the counters it
//! reads (`metrics`). The sealer itself
//! needs the engine's internals and lives in `shard::seal`.

pub(crate) mod crc32c;
pub(crate) mod directory;
pub(crate) mod estimate;
pub(crate) mod metrics;
pub(crate) mod object;
pub(crate) mod pending;

#[cfg(test)]
mod crc32c_tests;
#[cfg(test)]
mod directory_tests;
#[cfg(test)]
mod estimate_tests;
#[cfg(test)]
mod object_tests;
#[cfg(test)]
mod pending_tests;
#[cfg(test)]
pub(crate) mod test_pages;
