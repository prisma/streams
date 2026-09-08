//! Application contracts shared by protocol adapters and background owners.
pub(crate) mod read;
pub(crate) mod read_remote;

pub(crate) mod topology;

pub(crate) mod lifecycle;

pub(crate) mod append;

pub(crate) mod creation;
pub(crate) mod request_work;

pub(crate) mod consumer;
mod consumer_remote;

pub(crate) mod names;

pub(crate) mod watch;

pub(crate) mod read_scan;

pub(crate) mod read_budget;
mod read_wire;

pub(crate) mod read_keys;

#[cfg(test)]
pub(crate) mod read_retention_probe;

pub(crate) mod read_batch;
mod read_decode;
pub(crate) mod read_range;
