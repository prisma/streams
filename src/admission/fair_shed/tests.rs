use super::{EPOCH_MS, InstanceLoad, ProjectLoad, Verdict, Window, hard_line, judge, over_share};

/// `n` requests of `project` at `now_ms`, then its verdict.
fn charge_n(cell: &InstanceLoad, project: &ProjectLoad, n: u64, k: u64, now_ms: i64) -> Verdict {
    for _ in 0..n {
        cell.count(project, now_ms);
    }
    cell.judge(project, k, now_ms)
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
    assert!(!over_share(10, 80, 8), "exactly 1/8");
    assert!(over_share(11, 80, 8));
    assert!(!over_share(80, 80, 1), "k = 1: a dedicated cell");
    assert!(
        !over_share(81, 80, 1),
        "k = 1 never, even when a count races the cell's"
    );
    assert!(!over_share(80, 80, 0));
    assert!(over_share(2, 3, 2));
}

#[test]
fn judging_blames_the_heavy_spares_the_rest_and_evens_without_one() {
    assert_eq!(judge(11, 11, 80, 8), Verdict::Blamed);
    assert_eq!(judge(10, 11, 80, 8), Verdict::Spared);
    assert_eq!(judge(10, 10, 80, 8), Verdict::Even);
    assert_eq!(judge(80, 80, 80, 1), Verdict::Even, "dedicated");
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
fn the_hard_line_is_a_quarter_over_the_shed_line() {
    assert_eq!(
        [
            hard_line(500),
            hard_line(3),
            hard_line(0),
            hard_line(u64::MAX)
        ],
        [625, 3, 0, u64::MAX]
    );
}
