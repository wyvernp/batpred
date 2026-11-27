# -----------------------------------------------------------------------------
# Predbat Home Battery System
# Copyright Trefor Southwell 2026 - All Rights Reserved
# This application maybe used for personal use only and not for commercial use
# -----------------------------------------------------------------------------
# fmt off
# pylint: disable=consider-using-f-string
# pylint: disable=line-too-long
# pylint: disable=attribute-defined-outside-init

from const import EXPORT_MODE_IDLE, MINUTE_WATT, PREDICT_STEP
from prediction import Prediction
from tests.test_infra import reset_inverter
from utils import dp2, export_mode_of, pack_export_limit


def _setup_agile_scenario(
    my_predbat,
    rate_import_fn,
    rate_export_fn,
    soc_max=10.0,
    soc_kw=10.0,
    load_kw=0.5,
    pv_kw=0.0,
    charge_kw=3.0,
    discharge_kw=3.0,
    battery_loss=0.95,
    battery_loss_discharge=0.95,
    inverter_loss=0.95,
    cycle_cost=2.0,
    grid_import_limit_w=0.0,
    forecast_hours=12,
):
    """Configure a deterministic Agile tariff scenario on my_predbat and return the initial metric."""
    reset_inverter(my_predbat)
    forecast_minutes = forecast_hours * 60
    my_predbat.minutes_now = 0
    my_predbat.forecast_minutes = forecast_minutes
    my_predbat.end_record = forecast_minutes
    my_predbat.soc_max = soc_max
    my_predbat.soc_kw = soc_kw
    my_predbat.reserve = 0.5
    my_predbat.best_soc_min = 0.5
    my_predbat.best_soc_keep = 0.0
    my_predbat.best_soc_keep_weight = 0.0
    my_predbat.battery_rate_max_charge = charge_kw / 60.0
    my_predbat.battery_rate_max_charge_dc = charge_kw / 60.0
    my_predbat.battery_rate_max_discharge = discharge_kw / 60.0
    my_predbat.battery_rate_max_export = discharge_kw / 60.0
    my_predbat.battery_rate_max_scaling = 1.0
    my_predbat.battery_rate_max_scaling_discharge = 1.0
    my_predbat.inverter_limit = max(charge_kw, discharge_kw, 5.0) / 60.0
    my_predbat.export_limit = max(discharge_kw, 5.0) / 60.0
    my_predbat.inverter_hybrid = True
    my_predbat.battery_loss = battery_loss
    my_predbat.battery_loss_discharge = battery_loss_discharge
    my_predbat.inverter_loss = inverter_loss
    my_predbat.metric_battery_cycle = cycle_cost
    my_predbat.metric_min_improvement = 0.0
    my_predbat.metric_min_improvement_export = 0.1
    my_predbat.metric_min_improvement_export_freeze = 0.1
    my_predbat.pv_metric10_weight = 0.0
    my_predbat.pv_metric90_weight = 0.0
    my_predbat.calculate_best_charge = True
    my_predbat.calculate_best_export = True
    my_predbat.set_charge_window = True
    my_predbat.set_export_window = True
    my_predbat.set_charge_freeze = True
    my_predbat.set_charge_freeze_only = False
    my_predbat.set_export_freeze = True
    my_predbat.set_export_freeze_only = False
    my_predbat.set_export_low_power = False
    my_predbat.calculate_second_pass = True
    my_predbat.export_more_solar = False
    my_predbat.combine_charge_slots = False
    my_predbat.combine_export_slots = False
    my_predbat.charge_slot_split = 30
    my_predbat.export_slot_split = 30
    my_predbat.plan_interval_minutes = 30
    my_predbat.rate_low_threshold = 0.0
    my_predbat.rate_high_threshold = 0.0
    my_predbat.grid_import_limit = grid_import_limit_w / MINUTE_WATT
    my_predbat.manual_charge_times = []
    my_predbat.manual_export_times = []
    my_predbat.manual_demand_times = []
    my_predbat.manual_freeze_charge_times = []
    my_predbat.manual_freeze_export_times = []
    my_predbat.car_charging_slots = [[] for _ in range(max(my_predbat.num_cars, 1))]
    my_predbat.iboost_enable = False
    my_predbat.debug_enable = False

    rate_import = {}
    rate_export = {}
    pv_step = {}
    load_step = {}
    for minute in range(0, forecast_minutes + 12 * 60, PREDICT_STEP):
        rate_import[minute] = rate_import_fn(minute)
        rate_export[minute] = rate_export_fn(minute)
    for minute in range(0, forecast_minutes + PREDICT_STEP, PREDICT_STEP):
        pv_step[minute] = pv_kw * PREDICT_STEP / 60.0
        load_step[minute] = load_kw * PREDICT_STEP / 60.0

    my_predbat.rate_import = rate_import
    my_predbat.rate_export = rate_export
    my_predbat.rate_import_saving_minutes = set()
    my_predbat.rate_import_pre_saving = {}
    my_predbat.rate_export_saving_minutes = set()
    my_predbat.rate_export_pre_saving = {}
    my_predbat.rate_min, my_predbat.rate_max, my_predbat.rate_average, _, _ = my_predbat.rate_minmax(rate_import)
    my_predbat.rate_import_tariff_max = my_predbat.rate_max
    my_predbat.rate_export_min, my_predbat.rate_export_max, my_predbat.rate_export_average, _, _ = my_predbat.rate_minmax(rate_export)
    my_predbat.rate_export_max_forward = my_predbat.rate_export_max_forward_calc(rate_export)
    my_predbat.rate_min_forward = my_predbat.rate_min_forward_calc(rate_import)

    my_predbat.set_rate_thresholds()
    my_predbat.high_export_rates, _, _ = my_predbat.rate_scan_window(rate_export, 5, my_predbat.rate_export_cost_threshold, True, alt_rates=rate_import)
    my_predbat.find_low_rate_windows(alt_rates=rate_export)

    my_predbat.charge_window_best = [dict(w) for w in my_predbat.low_rates]
    my_predbat.charge_limit_best = [0.0 for _ in my_predbat.charge_window_best]
    my_predbat.export_window_best = [dict(w) for w in my_predbat.high_export_rates]
    my_predbat.export_limits_best = [pack_export_limit(EXPORT_MODE_IDLE) for _ in my_predbat.export_window_best]

    my_predbat.load_minutes_step = load_step
    my_predbat.load_minutes_step10 = load_step
    my_predbat.load_minutes_step90 = load_step
    my_predbat.pv_forecast_minute_step = pv_step
    my_predbat.pv_forecast_minute10_step = pv_step
    my_predbat.pv_forecast_minute90_step = pv_step
    my_predbat.prediction = Prediction(my_predbat, pv_step, pv_step, load_step, load_step)

    metric, _, _, _, _, _, _, _, metric_keep, _, _ = my_predbat.run_prediction(
        my_predbat.charge_limit_best,
        my_predbat.charge_window_best,
        my_predbat.export_window_best,
        my_predbat.export_limits_best,
        False,
        end_record=forecast_minutes,
    )
    return metric, metric_keep


def _run_pair(my_predbat, setup_kwargs):
    """Run both the baseline optimiser (without headroom pass) and the improved optimiser on the same scenario."""
    # 1. Baseline (without headroom pass)
    orig_fn = my_predbat.optimise_negative_import_headroom
    try:
        my_predbat.optimise_negative_import_headroom = lambda bm, bc, bk, bcy, bca, bi, debug_mode=False: (bm, bc, bk, bcy, bca, bi)
        init_metric, init_keep = _setup_agile_scenario(my_predbat, **setup_kwargs)
        base_res = my_predbat.optimise_all_windows(init_metric, init_keep, debug_mode=False)
        base_cost, base_imp_b, base_imp_h, base_exp, _, base_soc, _, base_cycle, _, _, _ = my_predbat.run_prediction(
            my_predbat.charge_limit_best,
            my_predbat.charge_window_best,
            my_predbat.export_window_best,
            my_predbat.export_limits_best,
            False,
            end_record=my_predbat.end_record,
        )
        base_active_exports = [
            (w["start"], w["end"], lim)
            for w, lim in zip(my_predbat.export_window_best, my_predbat.export_limits_best)
            if export_mode_of(lim) != EXPORT_MODE_IDLE
        ]
    finally:
        my_predbat.optimise_negative_import_headroom = orig_fn

    # 2. Improved (with headroom pass)
    init_metric, init_keep = _setup_agile_scenario(my_predbat, **setup_kwargs)
    new_res = my_predbat.optimise_all_windows(init_metric, init_keep, debug_mode=False)
    new_cost, new_imp_b, new_imp_h, new_exp, _, new_soc, _, new_cycle, _, _, _ = my_predbat.run_prediction(
        my_predbat.charge_limit_best,
        my_predbat.charge_window_best,
        my_predbat.export_window_best,
        my_predbat.export_limits_best,
        False,
        end_record=my_predbat.end_record,
    )
    new_active_exports = [
        (w["start"], w["end"], lim)
        for w, lim in zip(my_predbat.export_window_best, my_predbat.export_limits_best)
        if export_mode_of(lim) != EXPORT_MODE_IDLE
    ]
    return {
        "base": {"metric": base_res[0], "cost": base_cost, "soc": base_soc, "cycle": base_cycle, "export_kwh": base_exp, "exports": base_active_exports},
        "new": {"metric": new_res[0], "cost": new_cost, "soc": new_soc, "cycle": new_cycle, "export_kwh": new_exp, "exports": new_active_exports},
    }


def test_zero_export_negative_import(my_predbat):
    """Flat 0p export before a -15p/kWh negative import run should pre-discharge and cut net electricity cost."""
    res = _run_pair(
        my_predbat,
        {
            "rate_import_fn": lambda m: -15.0 if 120 <= m < 240 else 25.0,
            "rate_export_fn": lambda m: 0.0,
            "soc_max": 10.0,
            "soc_kw": 10.0,
            "load_kw": 0.5,
            "cycle_cost": 2.0,
        },
    )
    print(
        "  zero_export_negative_import: base cost {}p metric {}p (exp {}kWh) -> new cost {}p metric {}p (exp {}kWh)".format(
            dp2(res["base"]["cost"]), dp2(res["base"]["metric"]), dp2(res["base"]["export_kwh"]), dp2(res["new"]["cost"]), dp2(res["new"]["metric"]), dp2(res["new"]["export_kwh"])
        )
    )
    if not res["new"]["exports"]:
        print("ERROR: zero_export_negative_import expected pre-discharge before negative window, got none")
        return True
    if res["new"]["cost"] >= res["base"]["cost"] - 10.0:
        print("ERROR: zero_export_negative_import did not materially reduce net cost: base {} vs new {}".format(dp2(res["base"]["cost"]), dp2(res["new"]["cost"])))
        return True
    if res["new"]["metric"] >= res["base"]["metric"]:
        print("ERROR: zero_export_negative_import did not improve metric: base {} vs new {}".format(dp2(res["base"]["metric"]), dp2(res["new"]["metric"])))
        return True
    return False


def test_negative_export_deep_negative_import(my_predbat):
    """Paying -3p/kWh to export before a -20p/kWh import run is profitable and should pre-discharge."""
    res = _run_pair(
        my_predbat,
        {
            "rate_import_fn": lambda m: -20.0 if 120 <= m < 240 else 25.0,
            "rate_export_fn": lambda m: -3.0,
            "soc_max": 10.0,
            "soc_kw": 10.0,
            "load_kw": 0.5,
            "cycle_cost": 2.0,
        },
    )
    print(
        "  negative_export_deep_negative_import: base cost {}p metric {}p -> new cost {}p metric {}p (exp {}kWh)".format(
            dp2(res["base"]["cost"]), dp2(res["base"]["metric"]), dp2(res["new"]["cost"]), dp2(res["new"]["metric"]), dp2(res["new"]["export_kwh"])
        )
    )
    if not res["new"]["exports"]:
        print("ERROR: negative_export_deep_negative_import expected profitable pre-discharge, got none")
        return True
    if res["new"]["cost"] >= res["base"]["cost"] - 10.0 or res["new"]["metric"] >= res["base"]["metric"]:
        print("ERROR: negative_export_deep_negative_import failed to improve cost/metric")
        return True
    return False


def test_negative_export_unprofitable_no_discharge(my_predbat):
    """Paying -10p/kWh to export before a -5p/kWh import run loses money and must never force discharge."""
    res = _run_pair(
        my_predbat,
        {
            "rate_import_fn": lambda m: -5.0 if 120 <= m < 240 else 25.0,
            "rate_export_fn": lambda m: -10.0,
            "soc_max": 10.0,
            "soc_kw": 10.0,
            "load_kw": 0.5,
            "cycle_cost": 2.0,
        },
    )
    print("  negative_export_unprofitable: new exports={} base metric={} new metric={}".format(len(res["new"]["exports"]), dp2(res["base"]["metric"]), dp2(res["new"]["metric"])))
    if res["new"]["exports"]:
        print("ERROR: negative_export_unprofitable forced discharge when financially worse: {}".format(res["new"]["exports"]))
        return True
    return False


def test_natural_headroom_sufficient_no_discharge(my_predbat):
    """When battery already has enough natural headroom for the entire negative window, never force discharge."""
    res = _run_pair(
        my_predbat,
        {
            "rate_import_fn": lambda m: -15.0 if 120 <= m < 240 else 25.0,
            "rate_export_fn": lambda m: 0.0,
            "soc_max": 10.0,
            "soc_kw": 3.0,  # 7 kWh headroom + 1 kWh load before 02:00 > 6 kWh max 2h charge
            "load_kw": 0.5,
            "cycle_cost": 2.0,
        },
    )
    print("  natural_headroom_sufficient: new exports={} base cost={} new cost={}".format(len(res["new"]["exports"]), dp2(res["base"]["cost"]), dp2(res["new"]["cost"])))
    if res["new"]["exports"]:
        print("ERROR: natural_headroom_sufficient forced unnecessary discharge: {}".format(res["new"]["exports"]))
        return True
    return False


def test_high_cycle_cost_unprofitable_no_discharge(my_predbat):
    """When battery cycling cost exceeds the negative-import gain, never force discharge."""
    res = _run_pair(
        my_predbat,
        {
            "rate_import_fn": lambda m: -8.0 if 120 <= m < 240 else 25.0,
            "rate_export_fn": lambda m: 0.0,
            "soc_max": 10.0,
            "soc_kw": 10.0,
            "load_kw": 0.5,
            "cycle_cost": 10.0,
        },
    )
    print("  high_cycle_cost_unprofitable: new exports={} base metric={} new metric={}".format(len(res["new"]["exports"]), dp2(res["base"]["metric"]), dp2(res["new"]["metric"])))
    if res["new"]["exports"]:
        print("ERROR: high_cycle_cost_unprofitable forced discharge despite high cycle cost: {}".format(res["new"]["exports"]))
        return True
    return False


def test_grid_import_limit_forecast_cap(my_predbat):
    """Forecast-only grid_import_limit throttles grid battery charging without erasing household load or DC PV."""
    # 1. Unlimited vs 2 kW cap with 1 kW house load and 3 kW battery charge: battery should throttle to ~1 kW AC
    _setup_agile_scenario(
        my_predbat,
        rate_import_fn=lambda m: -10.0,
        rate_export_fn=lambda m: 0.0,
        soc_max=10.0,
        soc_kw=2.0,
        load_kw=1.0,
        pv_kw=0.0,
        charge_kw=3.0,
        battery_loss=1.0,
        inverter_loss=1.0,
        grid_import_limit_w=0.0,
        forecast_hours=2,
    )
    charge_win = [{"start": 0, "end": 120, "average": -10.0}]
    _, imp_bat_unlim, _, _, _, soc_unlim, _, _, _, _, _ = my_predbat.run_prediction([10.0], charge_win, [], [], False, end_record=120)

    _setup_agile_scenario(
        my_predbat,
        rate_import_fn=lambda m: -10.0,
        rate_export_fn=lambda m: 0.0,
        soc_max=10.0,
        soc_kw=2.0,
        load_kw=1.0,
        pv_kw=0.0,
        charge_kw=3.0,
        battery_loss=1.0,
        inverter_loss=1.0,
        grid_import_limit_w=2000.0,
        forecast_hours=2,
    )
    _, imp_bat_cap, _, _, _, soc_cap, _, _, _, _, _ = my_predbat.run_prediction([10.0], charge_win, [], [], False, end_record=120)

    # Unlimited: 2h * (1kW load + 3kW charge) = 8 kWh import, SoC 2 -> 8 kWh
    # Capped at 2 kW: 2h * 2kW = 4 kWh total import (2 kWh house + 2 kWh battery), SoC 2 -> 4 kWh
    if abs(soc_unlim - 8.0) > 0.1 or abs(soc_cap - 4.0) > 0.1 or abs(imp_bat_cap - 4.0) > 0.1:
        print("ERROR: grid_import_limit throttling mismatch: unlim soc={} imp={}, cap soc={} imp={}".format(dp2(soc_unlim), dp2(imp_bat_unlim), dp2(soc_cap), dp2(imp_bat_cap)))
        return True

    # 2. When household load alone (3 kW) exceeds the 2 kW cap, household load must NOT be erased and DC PV (1 kW) must still charge the battery
    _setup_agile_scenario(
        my_predbat,
        rate_import_fn=lambda m: -10.0,
        rate_export_fn=lambda m: 0.0,
        soc_max=10.0,
        soc_kw=2.0,
        load_kw=3.0,
        pv_kw=1.0,
        charge_kw=3.0,
        battery_loss=1.0,
        inverter_loss=1.0,
        grid_import_limit_w=2000.0,
        forecast_hours=2,
    )
    _, imp_bat_over, _, _, _, soc_over, _, _, _, _, _ = my_predbat.run_prediction([10.0], charge_win, [], [], False, end_record=120)
    # 2h * 3kW house load = 6 kWh still imported; 2h * 1kW DC PV still charges battery 2 -> 4 kWh
    if abs(imp_bat_over - 6.0) > 0.1 or abs(soc_over - 4.0) > 0.1:
        print("ERROR: grid_import_limit altered house load or DC PV: soc={} imp={}".format(dp2(soc_over), dp2(imp_bat_over)))
        return True
    print("  grid_import_limit_forecast_cap: PASSED (unlim soc={}, cap soc={}, over-load imp={} soc={})".format(dp2(soc_unlim), dp2(soc_cap), dp2(imp_bat_over), dp2(soc_over)))
    return False


def run_negative_import_headroom_tests(my_predbat):
    """Run coupled negative-import headroom and forecast-only grid_import_limit tests."""
    failed = False
    failed |= test_zero_export_negative_import(my_predbat)
    failed |= test_negative_export_deep_negative_import(my_predbat)
    failed |= test_negative_export_unprofitable_no_discharge(my_predbat)
    failed |= test_natural_headroom_sufficient_no_discharge(my_predbat)
    failed |= test_high_cycle_cost_unprofitable_no_discharge(my_predbat)
    failed |= test_grid_import_limit_forecast_cap(my_predbat)
    return failed
