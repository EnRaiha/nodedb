// SPDX-License-Identifier: BUSL-1.1

//! Outbound response-ring occupancy as an intake-throttle input.

use nodedb_mem::EngineId;

use super::helpers::{
    RESPONSES_ABOVE_SUSPEND, RESPONSES_ABOVE_THROTTLE, SUSPENDED_READ_DEPTH, fill_response_ring,
    make_governor_at, normal_depth,
};

#[test]
fn response_ring_saturation_throttles_read_depth() {
    let (mut core, mut tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    fill_response_ring(&mut core, &mut tx, RESPONSES_ABOVE_THROTTLE);

    core.apply_spsc_pressure();

    assert_eq!(
        core.spsc_read_depth(),
        normal_depth() / 2,
        "a backed-up response ring must throttle intake"
    );
    assert!(
        !core.pressure_suspend_reads(),
        "the throttle threshold must not suspend intake"
    );
}

#[test]
fn response_ring_saturation_suspends_intake() {
    let (mut core, mut tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    fill_response_ring(&mut core, &mut tx, RESPONSES_ABOVE_SUSPEND);

    core.apply_spsc_pressure();

    assert!(
        core.pressure_suspend_reads(),
        "a response ring past the suspend threshold must stop intake"
    );
    assert_eq!(core.spsc_read_depth(), SUSPENDED_READ_DEPTH);
}

#[test]
fn drained_response_ring_releases_the_throttle() {
    let (mut core, mut tx, mut rx, _dir) = crate::cases::core_loop::helpers::make_core();
    fill_response_ring(&mut core, &mut tx, RESPONSES_ABOVE_THROTTLE);
    core.apply_spsc_pressure();
    assert_eq!(core.spsc_read_depth(), normal_depth() / 2);

    while rx.try_pop().is_ok() {}
    for _ in 0..8 {
        core.apply_spsc_pressure();
    }

    assert_eq!(
        core.spsc_read_depth(),
        normal_depth(),
        "a drained response ring restores full intake"
    );
}

#[test]
fn memory_pressure_and_response_ring_take_the_stricter_level() {
    let (mut core, mut tx, _rx, _dir) = crate::cases::core_loop::helpers::make_core();
    fill_response_ring(&mut core, &mut tx, RESPONSES_ABOVE_SUSPEND);
    core.set_governor_for_testing(make_governor_at(EngineId::Vector, 50));

    core.apply_spsc_pressure();

    assert!(
        core.pressure_suspend_reads(),
        "a calm memory budget must not relax a saturated response ring"
    );
}
