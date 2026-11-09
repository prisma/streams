//! Boot-time bound on `SHARDS_PER_INSTANCE_MAX` (Q4).
//!
//! A split holds the parent and both its children on the instance that runs it, so an instance
//! must be allowed at least three shards; a smaller ceiling would refuse every split
//! (`409 instance_full`) and is refused at boot instead.

use crate::config::CliArgs;

/// The least ceiling a split fits under: the parent and its two children.
const MIN_SHARDS_PER_INSTANCE: u32 = 3;

/// The refusal of a ceiling no split fits under; empty when one does.
pub(crate) fn validate_shards_per_instance(cli: &CliArgs) -> Vec<String> {
    if cli.shards_per_instance_max >= MIN_SHARDS_PER_INSTANCE {
        return Vec::new();
    }
    vec![format!(
        "SHARDS_PER_INSTANCE_MAX={} is below {MIN_SHARDS_PER_INSTANCE}: a split holds the parent \
         and both its children on one instance, so no split would fit",
        cli.shards_per_instance_max
    )]
}

#[cfg(test)]
mod tests {
    use super::validate_shards_per_instance;
    use crate::config::CliArgs;

    /// Three shards (a parent and its two children) is the least; two is refused, naming the
    /// variable and why.
    #[test]
    fn a_ceiling_below_a_parent_and_its_two_children_is_refused() {
        let mut cli = CliArgs::deterministic();
        assert!(
            validate_shards_per_instance(&cli).is_empty(),
            "the default 7"
        );
        cli.shards_per_instance_max = 3;
        assert!(validate_shards_per_instance(&cli).is_empty());
        cli.shards_per_instance_max = 2;
        assert_eq!(
            validate_shards_per_instance(&cli),
            [
                "SHARDS_PER_INSTANCE_MAX=2 is below 3: a split holds the parent and both its \
              children on one instance, so no split would fit"
            ]
        );
    }
}
