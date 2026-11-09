//! Boot-time bound on the project tracker (tuning item 3b).
//!
//! `CONTAINER_TRACKER_MAX` sizes the instance's project tracker
//! (`quota::QuotaRegistry`), whose entries cost about 500 B each. A tracker
//! is allowed at most an eighth of the RSS shed line, so a large tracker
//! cannot by itself carry the instance to the line its writes are shed at;
//! the default 32,768 (~16 MiB) is allowed at any line, and is the bound
//! while the shed line is off, when the instance's memory is not known.

use crate::config::CliArgs;

/// The tracker's default size, allowed at any shed line.
pub(crate) const DEFAULT_PROJECT_TRACKER_MAX: u64 = 32_768;

/// One tracked project's estimated resident bytes.
const ENTRY_BYTES: u64 = 500;

/// The largest tracker an instance whose RSS shed line is `shed_mb` MiB may
/// hold: an eighth of the line at `ENTRY_BYTES` each, and never below the
/// default (which is also the bound with the line off, `shed_mb` 0).
pub(crate) fn project_tracker_bound(shed_mb: u64) -> u64 {
    if shed_mb == 0 {
        return DEFAULT_PROJECT_TRACKER_MAX;
    }
    let bound = shed_mb.saturating_mul(1 << 20) / 8 / ENTRY_BYTES;
    bound.max(DEFAULT_PROJECT_TRACKER_MAX)
}

/// The refusal of a tracker over its bound, naming both variables; empty
/// when it fits.
pub(crate) fn validate_project_tracker(cli: &CliArgs) -> Vec<String> {
    let bound = project_tracker_bound(cli.admit_rss_shed_mb);
    if cli.project_tracker_max <= bound {
        return Vec::new();
    }
    vec![format!(
        "CONTAINER_TRACKER_MAX={} exceeds {bound}, the most an instance with \
         ADMIT_RSS_SHED_MB={} may track: an eighth of the shed line at {ENTRY_BYTES} B a project \
         (the default {DEFAULT_PROJECT_TRACKER_MAX} at any line, and while the line is off)",
        cli.project_tracker_max, cli.admit_rss_shed_mb
    )]
}

#[cfg(test)]
mod tests {
    use super::{project_tracker_bound, validate_project_tracker};
    use crate::config::CliArgs;

    /// An eighth of a 500 MiB line is 131,072 projects and of a 128 MiB
    /// line 33,554; a line too small to fund the default, and a line that
    /// is off, allow exactly the default.
    #[test]
    fn the_bound_is_an_eighth_of_the_shed_line_and_never_below_the_default() {
        let bounds = [0, 100, 128, 500, 1_000].map(project_tracker_bound);
        assert_eq!(bounds, [32_768, 32_768, 33_554, 131_072, 262_144]);
        assert_eq!(project_tracker_bound(u64::MAX), u64::MAX / 8 / 500);
    }

    /// Boot takes the bound itself and refuses one project more, naming
    /// both variables.
    #[test]
    fn boot_refuses_a_tracker_one_over_its_bound() {
        let mut cli = CliArgs::deterministic();
        assert_eq!(
            (cli.admit_rss_shed_mb, cli.project_tracker_max),
            (500, 32_768)
        );
        assert!(validate_project_tracker(&cli).is_empty());
        cli.project_tracker_max = 131_072;
        assert!(validate_project_tracker(&cli).is_empty());
        cli.project_tracker_max = 131_073;
        assert_eq!(
            validate_project_tracker(&cli),
            [
                "CONTAINER_TRACKER_MAX=131073 exceeds 131072, the most an instance with \
              ADMIT_RSS_SHED_MB=500 may track: an eighth of the shed line at 500 B a project \
              (the default 32768 at any line, and while the line is off)"
            ]
        );
        cli.admit_rss_shed_mb = 0;
        cli.project_tracker_max = 32_769;
        assert_eq!(validate_project_tracker(&cli).len(), 1);
    }
}
