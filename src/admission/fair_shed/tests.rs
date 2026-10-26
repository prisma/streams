use super::{
    EPOCH_MS, InstanceLoad, ProjectLoad, Verdict, Weighted, Window, inflight_hard_line, judge,
    over_share, rss_hard_line,
};

/// `n` requests of `project` at weight 1 at `now_ms`, then its verdict.
fn charge_n(cell: &InstanceLoad, project: &ProjectLoad, n: u64, k: u64, now_ms: i64) -> Verdict {
    charge_weighted(cell, project, (n, 1), k, now_ms)
}

/// `n` requests of `project` at request weight `weight` at `now_ms`, then
/// its verdict at that weight.
fn charge_weighted(
    cell: &InstanceLoad,
    project: &ProjectLoad,
    (n, weight): (u64, u64),
    k: u64,
    now_ms: i64,
) -> Verdict {
    for _ in 0..n {
        cell.count(project, weight, now_ms);
    }
    cell.judge(project, weight, k, now_ms)
}

/// A load at weight 1.
fn unit(load: u64) -> Weighted {
    Weighted::new(load, 1)
}

#[test]
fn a_window_counts_this_epoch_and_the_one_before_it() {
    let mut w = Window::default();
    assert_eq!((w.count(7), w.count(7)), (1, 2));
    assert!(!w.roll(7) && !w.roll(6), "the same or an earlier epoch");
    assert_eq!(w.count(8), 3, "the next epoch keeps the previous count");
    assert_eq!((w.current, w.previous, w.epoch), (1, 2, 8));
    assert_eq!(w.count(9), 2, "the epoch before last is forgotten");
    assert!(w.roll(11));
    assert_eq!(w.load(), 0, "an epoch passed between");
}

#[test]
fn a_share_is_over_only_past_one_kth_and_never_on_a_dedicated_cell() {
    assert!(!over_share(unit(10), 80, 8), "exactly 1/8");
    assert!(over_share(unit(11), 80, 8));
    assert!(!over_share(unit(80), 80, 1), "k = 1: a dedicated cell");
    assert!(
        !over_share(unit(81), 80, 1),
        "k = 1 never, even when a count races the cell's"
    );
    assert!(!over_share(unit(80), 80, 0));
    assert!(over_share(unit(2), 3, 2));
    assert!(!over_share(Weighted::new(40, 4), 80, 8), "exactly 4/8");
    assert!(over_share(Weighted::new(41, 4), 80, 8));
    assert!(!over_share(Weighted::new(10, 0), 80, 8), "weight 0 is 1");
    assert!(over_share(Weighted::new(11, 0), 80, 8));
    let big = Weighted::new(u64::MAX, 7);
    assert!(
        over_share(big, u64::MAX, 8),
        "no overflow: 8 x max > 7 x max"
    );
    let within = Weighted::new(u64::MAX / 8 * 7, 7);
    assert!(!over_share(within, u64::MAX, 8), "7/8 of max at weight 7");
}

#[test]
fn judging_blames_the_heavy_spares_the_rest_and_evens_without_one() {
    assert_eq!(judge(unit(11), unit(11), 80, 8), Verdict::Blamed);
    assert_eq!(judge(unit(10), unit(11), 80, 8), Verdict::Spared);
    assert_eq!(judge(unit(10), unit(10), 80, 8), Verdict::Even);
    assert_eq!(judge(unit(80), unit(80), 80, 1), Verdict::Even, "dedicated");
    let heavy = Weighted::new(40, 4);
    assert_eq!(
        judge(heavy, unit(11), 80, 8),
        Verdict::Spared,
        "4/8 is its share"
    );
    assert_eq!(judge(heavy, heavy, 80, 8), Verdict::Even);
    assert_eq!(judge(Weighted::new(41, 4), unit(5), 80, 8), Verdict::Blamed);
}

#[test]
fn a_flooder_is_blamed_its_neighbour_spared_and_nine_equals_even() {
    let cell = InstanceLoad::default();
    let (flooder, neighbour) = (ProjectLoad::default(), ProjectLoad::default());
    let t = 3 * EPOCH_MS;
    assert_eq!(
        charge_n(&cell, &neighbour, 1, 8, t),
        Verdict::Blamed,
        "alone"
    );
    assert_eq!(charge_n(&cell, &flooder, 20, 8, t), Verdict::Blamed);
    assert_eq!(
        charge_n(&cell, &neighbour, 1, 8, t),
        Verdict::Spared,
        "2 of 22"
    );
    assert_eq!(charge_n(&cell, &neighbour, 1, 1, t), Verdict::Even, "k = 1");
    let even = InstanceLoad::default();
    let nine: Vec<ProjectLoad> = (0..9).map(|_| ProjectLoad::default()).collect();
    for _ in 0..10 {
        for p in &nine {
            charge_n(&even, p, 1, 8, t);
        }
    }
    let verdicts: Vec<Verdict> = nine.iter().map(|p| charge_n(&even, p, 1, 8, t)).collect();
    assert_eq!(
        verdicts,
        vec![Verdict::Even; 9],
        "nine equal projects: 11 x 8 <= 91"
    );
}

#[test]
fn the_heaviest_load_ages_out_with_its_window() {
    let cell = InstanceLoad::default();
    let (flooder, neighbour) = (ProjectLoad::default(), ProjectLoad::default());
    let t = 10 * EPOCH_MS;
    charge_n(&cell, &flooder, 40, 8, t);
    for _ in 0..4 {
        charge_n(&cell, &ProjectLoad::default(), 1, 8, t);
    }
    assert_eq!(charge_n(&cell, &neighbour, 1, 8, t), Verdict::Spared);
    let next = t + EPOCH_MS;
    for _ in 0..40 {
        charge_n(&cell, &ProjectLoad::default(), 1, 8, next);
    }
    assert_eq!(
        charge_n(&cell, &neighbour, 1, 8, next),
        Verdict::Spared,
        "the previous epoch's heaviest still counts: 40 x 8 > 86"
    );
    let later = next + EPOCH_MS;
    for _ in 0..40 {
        charge_n(&cell, &ProjectLoad::default(), 1, 8, later);
    }
    assert_eq!(
        charge_n(&cell, &neighbour, 1, 8, later),
        Verdict::Even,
        "aged out"
    );
    let gap = later + 3 * EPOCH_MS;
    assert_eq!(
        charge_n(&cell, &neighbour, 1, 8, gap),
        Verdict::Blamed,
        "alone again"
    );
    for _ in 0..16 {
        charge_n(&cell, &ProjectLoad::default(), 1, 8, gap);
    }
    assert_eq!(
        charge_n(&cell, &flooder, 1, 8, gap),
        Verdict::Even,
        "its old load forgotten"
    );
}

#[test]
fn the_rss_hard_line_is_an_eighth_over_the_shed_line() {
    assert_eq!(
        [
            rss_hard_line(500),
            rss_hard_line(1000),
            rss_hard_line(8),
            rss_hard_line(7),
            rss_hard_line(0),
            rss_hard_line(u64::MAX)
        ],
        [562, 1125, 9, 7, 0, u64::MAX]
    );
}

#[test]
fn the_inflight_hard_line_is_a_quarter_over_the_cap() {
    assert_eq!(
        [
            inflight_hard_line(500),
            inflight_hard_line(8),
            inflight_hard_line(3),
            inflight_hard_line(0),
            inflight_hard_line(u64::MAX)
        ],
        [625, 10, 3, 0, u64::MAX]
    );
}

/// The heaviest load is the heaviest per unit of weight: 40 requests at
/// weight 4 are lighter than 11 at weight 1, so beside them a unit project
/// is spared; ties keep the first counted.
#[test]
fn the_heaviest_load_is_the_heaviest_per_unit_of_weight() {
    let (light, heavy) = (Weighted::new(40, 4), unit(11));
    assert_eq!(light.heavier(heavy), heavy);
    assert_eq!(heavy.heavier(light), heavy);
    assert_eq!(unit(10).heavier(Weighted::new(40, 4)), unit(10), "a tie");
    assert_eq!(Weighted::new(9, 0), unit(9), "weight 0 is 1");
    let cell = InstanceLoad::default();
    let (weighted, flooder, calm) = (
        ProjectLoad::default(),
        ProjectLoad::default(),
        ProjectLoad::default(),
    );
    let t = 4 * EPOCH_MS;
    assert_eq!(
        charge_weighted(&cell, &weighted, (40, 4), 8, t),
        Verdict::Blamed,
        "alone, 40 of 40 is over 4/8"
    );
    assert_eq!(charge_n(&cell, &flooder, 50, 8, t), Verdict::Blamed);
    assert_eq!(
        charge_weighted(&cell, &weighted, (0, 4), 8, t),
        Verdict::Spared,
        "40 of 90 at weight 4 is within 4/8; the flooder's 50 is the heaviest"
    );
    assert_eq!(charge_n(&cell, &calm, 1, 8, t), Verdict::Spared, "1 of 91");
    assert_eq!(
        charge_weighted(&cell, &weighted, (0, 1), 8, t),
        Verdict::Blamed,
        "the same 40 at weight 1"
    );
}
