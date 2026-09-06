//! Application contracts shared by protocol adapters and background owners.
pub(crate) mod read;
pub(crate) mod read_remote;

pub(crate) mod topology;

pub(crate) mod lifecycle;

pub(crate) mod append;

pub(crate) mod creation;

pub(crate) mod consumer;
mod consumer_remote;

pub(crate) mod names;

pub(crate) mod watch;
