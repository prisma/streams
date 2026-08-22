//! The SSE subscription surface (LIVE-FEED transition, Stage 1+).
//! `auth` is the authoritative per-frame lease gate; `wire` owns the
//! SSE framing vocabulary. The LiveFeed engine lands in `feed`.

pub(crate) mod auth;
