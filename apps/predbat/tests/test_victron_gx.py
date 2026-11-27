"""Tests for the native Victron GX MQTT control path."""

import asyncio
import json
import time
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import patch

from inverter import Inverter
from victron import (
    MQTT_INVALID,
    VictronGX,
    _mqtt_value,
    _within,
    valid_watchdog_heartbeat,
    victron_setpoint,
    watchdog_signature,
)
from victron_watchdog import VictronWatchdog, _same, _valid_heartbeat


class FakeBase:
    """Minimal component base for focused Victron tests."""

    def __init__(self):
        self.log_messages = []
        self.local_tz = timezone.utc
        self.prefix = "predbat"
        self.args = {}
        self.set_read_only = False
        self.published_states = {}

    def log(self, message):
        """Record component logs."""
        self.log_messages.append(message)

    def set_state_wrapper(self, entity_id, state, attributes=None):
        """Record diagnostic sensor updates."""
        self.published_states[entity_id] = (state, attributes or {})


class FakeMQTT:
    """Fake broker that confirms W requests only with a subsequent N notification."""

    def __init__(self, component):
        self.component = component
        self.published = []

    async def publish(self, topic, payload, qos, retain):
        """Capture a publish and simulate the GX N notification for W requests."""
        self.published.append((topic, payload, qos, retain))
        if topic.startswith("W/"):
            key = next((name for name, path in self.component._write_paths.items() if path.replace("N/", "W/", 1) == topic), None)
            if key:
                value = json.loads(payload)["value"]
                self.component._latest[key] = {"value": value, "at": time.monotonic()}
                self.component._pending[key]["future"].set_result(True)
                if key == "setpoint" and value is not None and getattr(self, "invalidate_after_setpoint", False):
                    self.component._latest["allow_charge"]["value"] = 0


def _config(**overrides):
    """Return a valid explicit test configuration."""
    config = {
        "host": "gx.local",
        "portal_id": "test-portal-id",
        "hub4_instance": 0,
        "vebus_instance": 257,
        "settings_instance": 0,
        "system_instance": 0,
        "battery_instance": 0,
        "battery_service_type": "battery",
        "grid_input": 0,
        "dess_off_value": 0,
        "charge_setpoint_w": 2400,
        "export_setpoint_w": 2500,
        "max_grid_import_w": 3000,
        "max_grid_export_w": 3000,
        "max_charge_current_a": 70,
        "max_discharge_power_w": 4000,
        "enable_controls": True,
        "watchdog_required": True,
        "watchdog_token": "a" * 40,
        "watchdog_safe_max_charge_current_a": 60,
        "watchdog_safe_max_discharge_power_w": 3000,
        "tls": False,
    }
    config.update(overrides)
    return config


def _ready_component(**overrides):
    """Build a component with a fresh, complete set of safe telemetry."""
    component = VictronGX(FakeBase(), config=_config(**overrides))
    now = time.monotonic()
    values = {
        "active_input": 0,
        "active_source": 1,
        "hub4_mode": 2,
        "dess_mode": 0,
        "allow_charge": 1,
        "allow_discharge": 1,
        "battery_soc": 50,
        "battery_voltage": 52,
        "bms_max_charge_current": 70,
        "bms_max_discharge_current": 100,
        "bms_expected": 1,
        "max_charge_current": 70,
        "max_discharge_power": 4000,
        "setpoint": 0,
    }
    component._latest = {name: {"value": value, "at": now} for name, value in values.items()}
    component._connected = True
    component._full_publish_complete = True
    component._watchdog_alive_monotonic = now
    return component


class FakeVictronControl:
    """Record the standard inverter controls routed through the GX component."""

    def __init__(self):
        self.calls = []

    def set_charge_power_sync(self, power_w):
        """Record a charge-rate command."""
        self.calls.append(("charge", power_w))
        return True

    def set_discharge_power_sync(self, power_w):
        """Record a discharge-rate command."""
        self.calls.append(("discharge", power_w))
        return True

    def set_operation_sync(self, target_soc, charging, exporting):
        """Record a Mode 2 operation command."""
        self.calls.append(("operation", target_soc, charging, exporting))
        return True


class FakeWatchdogMQTT:
    """Simulate the broker forwarding matching GX N notifications after W requests."""

    def __init__(self, watchdog, reject=False):
        self.watchdog = watchdog
        self.reject = reject
        self.published = []

    async def publish(self, topic, payload, qos, retain):
        """Record a non-retained request and deliver its matching notification."""
        self.published.append((topic, payload, qos, retain))
        if topic.startswith("W/"):
            await asyncio.sleep(0.001)
            if self.reject:
                name = next(
                    (key for key, (service, instance, path, _value) in self.watchdog._paths().items() if topic == self.watchdog._topic("W", service, instance, path)),
                    None,
                )
                self.watchdog.pending[name]["future"].set_result(False)
                return
            self.watchdog._handle_message(SimpleNamespace(topic=topic.replace("W/", "N/", 1), payload=payload))


class FakeWatchdogStreamClient:
    """Provide a finite, non-blocking MQTT stream to the watchdog's real connected loop."""

    def __init__(self, messages):
        self.messages = self
        self.pending_messages = list(messages)
        self.published = []

    async def __anext__(self):
        if not self.pending_messages:
            raise StopAsyncIteration
        return self.pending_messages.pop(0)

    async def subscribe(self, _topic, qos):
        assert qos == 0

    async def publish(self, topic, payload, qos, retain):
        self.published.append((topic, payload, qos, retain))


def run_victron_gx_tests(_predbat=None):
    """Run focused parser, interlock, setpoint, watchdog, and MQTT-ack tests."""
    assert _mqtt_value(b'{"value":12.5}') == 12.5
    assert _mqtt_value(b'{"value":null}') is None
    assert _mqtt_value(b"not-json") is MQTT_INVALID
    assert _mqtt_value(b'{"value":NaN}') is MQTT_INVALID
    assert _mqtt_value(b"{}") is MQTT_INVALID
    assert _within("10", 10)
    assert not _within("unavailable", 10)

    assert victron_setpoint(80, 50, True, False, 2400, 3000) == 2400
    assert victron_setpoint(20, 50, False, True, 2400, 3000) == -3000
    assert victron_setpoint(50, 50, True, False, 2400, 3000) is None
    assert victron_setpoint(20, 50, False, False, 2400, 3000) is None
    assert victron_setpoint(90, 50, True, True, 2400, 3000) is None

    timestamp = 1000
    binding = {
        "portal_id": "test-portal-id",
        "host": "gx.local",
        "port": 8883,
        "hub4_instance": 0,
        "settings_instance": 0,
        "heartbeat_topic": "predbat/predbat/victron/test-portal-id/heartbeat",
        "safe_max_charge_current_a": 60.0,
        "safe_max_discharge_power_w": 3000,
    }
    signature = watchdog_signature("example-shared-secret", timestamp, binding)
    heartbeat = json.dumps({"timestamp": timestamp, "binding": binding, "healthy": True, "signature": signature})
    assert valid_watchdog_heartbeat(heartbeat, "example-shared-secret", binding, now=1010)
    assert not valid_watchdog_heartbeat(heartbeat, "wrong-secret", binding, now=1010)
    assert not valid_watchdog_heartbeat(heartbeat, "example-shared-secret", dict(binding, hub4_instance=1), now=1010)
    assert not valid_watchdog_heartbeat(heartbeat, "example-shared-secret", dict(binding, safe_max_charge_current_a=50), now=1010)
    assert not valid_watchdog_heartbeat(heartbeat, "example-shared-secret", binding, now=1200)
    assert not valid_watchdog_heartbeat("not-json", "example-shared-secret", binding, now=1010)
    assert _valid_heartbeat(heartbeat, "example-shared-secret", binding, now=1010)
    assert not _valid_heartbeat(heartbeat, "wrong-secret", binding, now=1010)
    unhealthy = json.dumps({"timestamp": timestamp, "binding": binding, "healthy": False, "signature": watchdog_signature("example-shared-secret", timestamp, binding, False)})
    assert not valid_watchdog_heartbeat(unhealthy, "example-shared-secret", binding, now=1010)

    component = _ready_component()
    assert component._control_ready()
    component._latest["setpoint"]["value"] = None
    assert component._control_ready()
    component._latest["setpoint"]["value"] = 0
    component.prefix = "house"
    component._publish_telemetry("battery_soc", 50)
    assert component.base.published_states["sensor.house_victron_soc"][0] == 50
    component._publish_telemetry("hub4_mode", 2)
    assert component.base.published_states["sensor.house_victron_hub4_mode"][0] == 2
    component._publish_control_sensor("max_charge_current", 70)
    assert component.base.published_states["sensor.house_victron_charge_rate"][0] == 3640
    component.prefix = "predbat"
    component._latest["active_input"]["value"] = 240
    assert not component._control_ready()
    component._latest["active_input"]["value"] = 1
    assert not component._control_ready()
    component._latest["active_input"]["value"] = 0
    component._latest["active_input"]["value"] = 0.5
    assert not component._control_ready()
    component._latest["active_input"]["value"] = 0
    component._latest["active_source"]["value"] = 2
    assert not component._control_ready()
    component._latest["active_source"]["value"] = 1
    component._latest["hub4_mode"]["value"] = 1
    assert not component._control_ready()
    # Hub4Mode is a separate phase-compensation setting, not the ESS Mode 2 control protocol.
    component._latest["hub4_mode"]["value"] = 3
    assert not component._control_ready()
    component._latest["hub4_mode"]["value"] = 2
    assert component._control_ready()
    component._latest["dess_mode"]["value"] = 1
    assert not component._control_ready()
    component._latest["dess_mode"]["value"] = 0
    component._latest["allow_charge"]["value"] = 0.5
    assert not component._control_ready()
    component._latest["allow_charge"]["value"] = 1
    component._latest["battery_soc"]["at"] -= 2 * 60
    assert not component._control_ready()

    async def check_connected_loop_cancellation_cleanup():
        """Cancelling the parent awaits cleanup of both MQTT worker tasks."""
        component = _ready_component()
        stopped = set()
        children = []

        async def blocked_worker(name):
            children.append(asyncio.current_task())
            try:
                await asyncio.Event().wait()
            finally:
                stopped.add(name)

        component._receive = lambda _client: blocked_worker("receiver")
        component._refresh = lambda _client: blocked_worker("refresher")

        async def scenario():
            task = asyncio.create_task(component._connected_loop(object()))
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            assert stopped == {"receiver", "refresher"}
            assert len(children) == 2 and all(child.done() for child in children)

        await asyncio.wait_for(scenario(), timeout=1)

    asyncio.run(check_connected_loop_cancellation_cleanup())

    try:
        VictronGX(FakeBase(), config=_config(watchdog_required=False))
        raise AssertionError("missing external watchdog must reject direct controls")
    except ValueError:
        pass

    assert _same("10", 10)
    assert not _same("unavailable", 10)
    watchdog = VictronWatchdog(
        {
            "host": "gx.local",
            "portal_id": "test-portal-id",
            "hub4_instance": 0,
            "settings_instance": 0,
            "heartbeat_topic": "predbat/predbat/victron/test-portal-id/heartbeat",
            "token": "b" * 40,
            "safe_max_charge_current_a": 60,
            "safe_max_discharge_power_w": 3000,
        }
    )
    assert watchdog._paths()["setpoint"] == ("hub4", 0, "Overrides/Setpoint", None)
    assert watchdog._paths()["max_charge_current"][-1] == 60

    async def check_watchdog_fallback():
        """The independent watchdog waits for notifications on all fallback writes."""
        watchdog.client = FakeWatchdogMQTT(watchdog)
        watchdog.last_fallback = time.monotonic()
        assert await watchdog._apply_fallback()
        assert json.loads(watchdog.client.published[0][1]) == {"value": None}
        assert [request[0] for request in watchdog.client.published] == [
            "W/test-portal-id/hub4/0/Overrides/Setpoint",
            "W/test-portal-id/settings/0/Settings/SystemSetup/MaxChargeCurrent",
            "W/test-portal-id/settings/0/Settings/CGwacs/MaxDischargePower",
        ]
        assert all(request[3] is False for request in watchdog.client.published)

    asyncio.run(check_watchdog_fallback())

    async def check_watchdog_failure_is_visible():
        """A missing GX confirmation marks the watchdog unhealthy and reports the failure."""
        failed_watchdog = VictronWatchdog(
            {
                "host": "gx.local",
                "portal_id": "test-portal-id",
                "hub4_instance": 0,
                "settings_instance": 0,
                "heartbeat_topic": "predbat/predbat/victron/test-portal-id/heartbeat",
                "token": "b" * 40,
                "safe_max_charge_current_a": 60,
                "safe_max_discharge_power_w": 3000,
            }
        )
        messages = []
        failed_watchdog.logger = SimpleNamespace(
            error=lambda message, *args: messages.append(message % args if args else message),
            info=lambda message, *args: messages.append(message % args if args else message),
        )
        failed_watchdog.client = FakeWatchdogMQTT(failed_watchdog, reject=True)
        failed_watchdog.last_fallback = time.monotonic()
        assert not await failed_watchdog._apply_fallback()
        assert failed_watchdog.fallback_failed and not failed_watchdog.healthy
        assert any("fallback" in message for message in messages)

    asyncio.run(check_watchdog_failure_is_visible())

    controls = FakeVictronControl()
    components = SimpleNamespace(get_component=lambda name: controls if name == "victron_gx_mqtt" else None)
    inverter = Inverter.__new__(Inverter)
    inverter.base = SimpleNamespace(components=components, log=lambda _message: None)
    inverter.inverter_type = "VICTRON_GX_MQTT"
    inverter.reserve_percent = 10
    inverter.inv_has_target_soc = True
    inverter.inv_target_soc_used_for_discharge = True
    assert inverter.adjust_charge_rate(1234) is True
    assert inverter.adjust_discharge_rate(2345) is True
    assert inverter.adjust_battery_target(80, True, False) is True
    assert controls.calls == [
        ("charge", 1234),
        ("discharge", 2345),
        ("operation", 80, True, False),
    ]

    async def check_notification_ack():
        """A W publish must wait for its matching later N notification."""
        component = _ready_component()
        component.client = FakeMQTT(component)
        component._latest["setpoint"]["value"] = None
        assert await component._write_verified("setpoint", 0)
        assert component.client.published == [("W/test-portal-id/hub4/0/Overrides/Setpoint", '{"value": 0}', 0, False)]
        assert component._latest["setpoint"]["value"] == 0

    asyncio.run(check_notification_ack())

    async def check_null_clear_without_stale_interlocks():
        """A neutral or read-only clear remains available when active-control telemetry is stale."""
        component = _ready_component()
        component.client = FakeMQTT(component)
        component._latest["active_input"]["at"] -= 2 * 60
        assert not component._control_ready()
        assert await component.set_operation(100, False, False)
        assert component.client.published == [
            ("W/test-portal-id/hub4/0/Overrides/Setpoint", '{"value": null}', 0, False),
        ]

    asyncio.run(check_null_clear_without_stale_interlocks())

    async def check_transition_verification():
        """Only commit the setpoint after both rate notifications and combined state verification."""
        component = _ready_component()
        component.client = FakeMQTT(component)
        assert await component.set_operation(80, True, False)
        assert component._pending_operation == 2400
        assert [request[0] for request in component.client.published] == [
            "W/test-portal-id/hub4/0/Overrides/Setpoint",
        ]
        assert await component.set_charge_power_w(2400)
        assert await component.set_discharge_power_w(4000)
        assert not any("heartbeat" in request[0] for request in component.client.published)
        assert await component.finalize_control_cycle()
        writes = [request[0] for request in component.client.published if request[0].startswith("W/")]
        assert writes == [
            "W/test-portal-id/hub4/0/Overrides/Setpoint",
            "W/test-portal-id/settings/0/Settings/SystemSetup/MaxChargeCurrent",
            "W/test-portal-id/hub4/0/Overrides/Setpoint",
        ]
        assert component._latest["setpoint"]["value"] == 2400
        assert any(request[0] == component.heartbeat_topic for request in component.client.published)

        rejected = _ready_component()
        rejected.client = FakeMQTT(rejected)
        rejected.client.invalidate_after_setpoint = True
        assert await rejected.set_operation(80, True, False)
        assert await rejected.set_charge_power_w(2400)
        assert await rejected.set_discharge_power_w(4000)
        assert not await rejected.finalize_control_cycle()
        assert rejected._latest["setpoint"]["value"] is None
        assert not any(request[0] == rejected.heartbeat_topic for request in rejected.client.published)

    asyncio.run(check_transition_verification())

    def check_charge_transition_uses_amperes():
        """A reduced managed-battery BMS current ceiling invalidates a charge rate in amperes."""
        component = _ready_component()
        component._latest["setpoint"]["value"] = 2400
        component._latest["max_charge_current"]["value"] = 60
        component._latest["max_discharge_power"]["value"] = 3000
        component._cycle_expected_rates = {"max_charge_current": 60, "max_discharge_power": 3000}
        component._latest["bms_max_charge_current"]["value"] = 50
        assert not component._transition_telemetry_matches(2400)
        component._latest["bms_max_charge_current"]["value"] = 70
        assert component._transition_telemetry_matches(2400)

    check_charge_transition_uses_amperes()

    async def check_neutral_cycles_refresh_watchdog_after_timeout():
        """Freeze/demand neutral cycles heartbeat only after both rates verify, even after 900s."""
        component = _ready_component()
        component.client = FakeMQTT(component)
        heartbeat_payloads = []
        token = component.watchdog_token
        timestamps = (1000, 1901)
        for timestamp in timestamps:
            with patch("victron.time.time", return_value=timestamp):
                assert await component.set_operation(50, False, False)
                assert await component.set_charge_power_w(2400)
                assert await component.set_discharge_power_w(3000)
                assert await component.finalize_control_cycle()
            heartbeat_payloads.append(next(payload for topic, payload, _qos, _retain in reversed(component.client.published) if topic == component.heartbeat_topic))
        assert not valid_watchdog_heartbeat(heartbeat_payloads[0], token, component.heartbeat_binding, now=1901)
        assert valid_watchdog_heartbeat(heartbeat_payloads[1], token, component.heartbeat_binding, now=1901)

        clock = {"now": 1901}
        watchdog = VictronWatchdog(
            {
                "host": component.host,
                "portal_id": component.portal_id,
                "hub4_instance": component.hub4_instance,
                "settings_instance": component.settings_instance,
                "heartbeat_topic": component.heartbeat_topic,
                "token": token,
                "safe_max_charge_current_a": component.watchdog_safe_charge_current_a,
                "safe_max_discharge_power_w": component.watchdog_safe_discharge_power_w,
            },
            monotonic=lambda: clock["now"],
            wall_time=lambda: clock["now"],
        )
        watchdog.connected_at = 0
        watchdog.last_heartbeat = 0
        watchdog.initial_fallback_pending = False
        client = FakeWatchdogStreamClient([SimpleNamespace(topic=component.heartbeat_topic, payload=heartbeat_payloads[1])])
        try:
            await watchdog._connected(client)
        except StopAsyncIteration:
            pass
        assert watchdog.last_heartbeat == 1901 and watchdog.healthy
        assert watchdog.fallback_task is None

        incomplete = _ready_component()
        incomplete.client = FakeMQTT(incomplete)
        assert await incomplete.set_operation(50, False, False)
        assert await incomplete.set_charge_power_w(2400)
        assert not await incomplete.finalize_control_cycle()
        assert not any(topic == incomplete.heartbeat_topic for topic, _payload, _qos, _retain in incomplete.client.published)

        read_only = _ready_component()
        read_only.client = FakeMQTT(read_only)
        read_only.base.set_read_only = True
        assert await read_only.set_operation(50, False, False)
        assert await read_only.finalize_control_cycle()
        assert not any(topic == read_only.heartbeat_topic for topic, _payload, _qos, _retain in read_only.client.published)

    asyncio.run(check_neutral_cycles_refresh_watchdog_after_timeout())

    async def check_vebus_bms_topology():
        """A declared VE.Bus BMS uses its own BMS flags instead of managed-battery Info topics."""
        component = _ready_component(battery_service_type="vebus")
        component._latest.pop("bms_max_charge_current")
        component._latest.pop("bms_max_discharge_current")
        assert component._control_ready()
        assert component._read_paths["allow_charge"] == "N/test-portal-id/vebus/257/Bms/AllowToCharge"
        assert component._read_paths["bms_expected"] == "N/test-portal-id/vebus/257/Bms/BmsExpected"
        component._latest["bms_expected"]["value"] = 0
        assert not component._control_ready()

    asyncio.run(check_vebus_bms_topology())

    async def check_fail_closed():
        """A stale or incomplete telemetry set must prevent all W requests."""
        component = _ready_component()
        component.client = FakeMQTT(component)
        component._latest["active_source"]["value"] = 3
        assert not await component.set_operation(80, True, False)
        assert component.client.published == []

    asyncio.run(check_fail_closed())
    return False
