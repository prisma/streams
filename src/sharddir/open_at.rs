//! The directory's opener and the shard log's path (S4 route A): the opener is handed the path the
//! cell's topology gives the shard's home, and a log's path names its home.

use super::{OpenAt, OpenFn, prefix_of_db_path};

/// The directory's opener: one that takes the log's path ([`OpenAt`], production's), or one that
/// derives its own from the prefix ([`OpenFn`], the tests' rigs).
pub(crate) struct LogOpener(pub(crate) OpenAt);

impl From<OpenAt> for LogOpener {
    fn from(open: OpenAt) -> Self {
        Self(open)
    }
}

impl From<OpenFn> for LogOpener {
    fn from(open: OpenFn) -> Self {
        Self(Box::new(move |prefix, _path, incarnation| {
            open(prefix, incarnation)
        }))
    }
}

/// The home a shard log's path names: an initial home's `shards/<prefix>` (`shards/root`) names
/// the prefix read as a number (`keyspace::home_of`); an operation-born home's
/// `shards/<prefix>-<home>` names its home. `None` for a path of neither form.
pub(crate) fn home_of_db_path(path: &str) -> Option<u32> {
    let named = prefix_of_db_path(path);
    let Some((prefix, home)) = named.split_once('-') else {
        return crate::keyspace::home_of(named);
    };
    let bits = !prefix.is_empty() && prefix.bytes().all(|b| b == b'0' || b == b'1');
    let digits = !home.is_empty() && home.bytes().all(|b| b.is_ascii_digit());
    (path.starts_with("shards/") && bits && digits)
        .then(|| home.parse().ok())
        .flatten()
}
