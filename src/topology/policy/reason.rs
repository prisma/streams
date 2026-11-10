//! A decision as the heartbeat publishes it (E6, edge change #261): space-separated
//! `key=value` segments an operator reads and the DSTs pin.
use super::{Decision, Hold, SplitKind};

impl Hold {
    /// The hold's name.
    fn name(self) -> &'static str {
        match self {
            Hold::Steady => "steady",
            Hold::PeerActs => "peer_acts",
            Hold::IntentLive => "intent_live",
            Hold::Cooldown => "cooldown",
            Hold::FleetSettling => "fleet_settling",
            Hold::OverrideRecent => "override_recent",
            Hold::HotEntity => "hot_entity",
            Hold::MaxDepth => "max_depth",
            Hold::TopologyAtMax => "topology_at_max",
            Hold::Congested => "congested",
            Hold::BeatsStale => "beats_stale",
        }
    }
}

impl Decision {
    /// The decision's segments; the root prefix "" reads as `root`.
    pub(crate) fn reason(&self) -> String {
        let named = |prefix: &str| {
            if prefix.is_empty() {
                "root".to_owned()
            } else {
                prefix.to_owned()
            }
        };
        match self {
            Decision::Split {
                prefix,
                kind,
                doubling,
                fleet_axis,
            } => {
                let kind = match kind {
                    SplitKind::Normal => "normal",
                    SplitKind::Emergency => "emergency",
                };
                let prefix = named(prefix);
                format!(
                    "split prefix={prefix} kind={kind} doubling={doubling} fleet_axis={fleet_axis}"
                )
            }
            Decision::Merge { parent } => format!("merge parent={}", named(parent)),
            Decision::AskInstance { resident, max } => {
                format!("instance_full shards_per_instance={resident}/{max}")
            }
            Decision::Hold(hold) => format!("hold={}", hold.name()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Decision, Hold, SplitKind};

    #[test]
    fn a_decision_reads_as_its_segments() {
        let split = Decision::Split {
            prefix: "01".into(),
            kind: SplitKind::Emergency,
            doubling: true,
            fleet_axis: 3,
        };
        assert_eq!(
            split.reason(),
            "split prefix=01 kind=emergency doubling=true fleet_axis=3"
        );
        let root = Decision::Split {
            prefix: String::new(),
            kind: SplitKind::Normal,
            doubling: false,
            fleet_axis: 0,
        };
        assert_eq!(
            root.reason(),
            "split prefix=root kind=normal doubling=false fleet_axis=0"
        );
        assert_eq!(
            Decision::Merge { parent: "1".into() }.reason(),
            "merge parent=1"
        );
        let ask = Decision::AskInstance {
            resident: 6,
            max: 7,
        };
        assert_eq!(ask.reason(), "instance_full shards_per_instance=6/7");
        let holds = [
            (Hold::Steady, "steady"),
            (Hold::PeerActs, "peer_acts"),
            (Hold::IntentLive, "intent_live"),
            (Hold::Cooldown, "cooldown"),
            (Hold::FleetSettling, "fleet_settling"),
            (Hold::OverrideRecent, "override_recent"),
            (Hold::HotEntity, "hot_entity"),
            (Hold::MaxDepth, "max_depth"),
            (Hold::TopologyAtMax, "topology_at_max"),
            (Hold::Congested, "congested"),
            (Hold::BeatsStale, "beats_stale"),
        ];
        for (hold, name) in holds {
            assert_eq!(Decision::Hold(hold).reason(), format!("hold={name}"));
        }
    }
}
