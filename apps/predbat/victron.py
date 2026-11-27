# -----------------------------------------------------------------------------
# Predbat Home Battery System
# Copyright Trefor Southwell 2026 - All Rights Reserved
# This application maybe used for personal use only and not for commercial use
# -----------------------------------------------------------------------------
# fmt off
# pylint: disable=consider-using-f-string
# pylint: disable=line-too-long

"""Direct local MQTT control for a Victron GX running Venus OS."""

import asyncio
import hashlib
import hmac
import json
import math
import re
import secrets
import ssl
import time

from component_base import ComponentBase, writes

try:
    import aiomqtt
except ImportError:
    aiomqtt = None


VICTRON_MQTT_PORT = 8883
VICTRON_MQTT_KEEPALIVE_SECONDS = 30
VICTRON_CONTROL_STALE_SECONDS = 60
VICTRON_WATCHDOG_STALE_SECONDS = 60
VICTRON_WRITE_ACK_TIMEOUT_SECONDS = 8
VICTRON_RECONNECT_MAX_SECONDS = 60
VICTRON_READ_REFRESH_SECONDS = 15
VICTRON_GRID_SOURCE = 1
VICTRON_DISCONNECTED_INPUT = 240
VICTRON_MAX_TOPIC_ID_LENGTH = 64
MQTT_INVALID = object()
MQTT_MISSING = object()


def _positive_number(value, name):
    """Return a finite positive number or raise a configuration error."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError("{} must be a positive number".format(name)) from None
    if not math.isfinite(number) or number <= 0:
        raise ValueError("{} must be a positive number".format(name))
    return number


def _nonnegative_number(value, name):
    """Return a finite non-negative number or raise a configuration error."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError("{} must be a non-negative number".format(name)) from None
    if not math.isfinite(number) or number < 0:
        raise ValueError("{} must be a non-negative number".format(name))
    return number


def _instance(value, name):
    """Return an explicit non-negative device instance number."""
    if isinstance(value, bool):
        raise ValueError("{} must be a non-negative integer".format(name))
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ValueError("{} must be a non-negative integer".format(name)) from None
    if result < 0 or str(value).strip() != str(result):
        raise ValueError("{} must be a non-negative integer".format(name))
    return result


def _mqtt_value(payload):
    """Extract one finite scalar from a dbus-flashmq JSON value."""
    try:
        decoded = json.loads(payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        return MQTT_INVALID
    if not isinstance(decoded, dict):
        return MQTT_INVALID
    if "value" not in decoded:
        return MQTT_INVALID
    value = decoded["value"]
    if value is None:
        return None
    if isinstance(value, bool):
        return MQTT_INVALID
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else MQTT_INVALID
    if isinstance(value, str) and value:
        return value
    return MQTT_INVALID


def _within(value, target, tolerance=0.1):
    """Whether a notification confirms a numeric target within its resolution."""
    try:
        return math.isfinite(float(value)) and abs(float(value) - float(target)) <= tolerance
    except (TypeError, ValueError):
        return False


def _is_finite_number(value):
    """Whether a value can safely be used as a finite numeric GX measurement."""
    if value is None or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def victron_setpoint(target_soc, current_soc, charging, exporting, charge_setpoint_w, export_setpoint_w):
    """Map a directional Predbat operation to an ESS Mode 2 override, or None to clear it."""
    if bool(charging) and bool(exporting):
        return None
    try:
        target_soc = float(target_soc)
        current_soc = float(current_soc)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(target_soc) or not math.isfinite(current_soc) or not 0 <= target_soc <= 100:
        return None
    if charging and target_soc > current_soc + 0.5:
        return int(round(charge_setpoint_w))
    if exporting and target_soc < current_soc - 0.5:
        return -int(round(export_setpoint_w))
    return None


def watchdog_signature(token, timestamp, binding, healthy=True):
    """Sign a timestamp and the exact GX/watchdog configuration with the shared secret."""
    data = json.dumps({"timestamp": int(timestamp), "binding": binding, "healthy": bool(healthy)}, sort_keys=True, separators=(",", ":"))
    return hmac.new(token.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def valid_watchdog_heartbeat(payload, token, binding, now=None, max_age=120):
    """Validate a recent heartbeat and require an exact GX and fallback configuration match."""
    try:
        message = json.loads(payload)
        timestamp = int(message["timestamp"])
        signature = message["signature"]
        received_binding = message["binding"]
        healthy = message["healthy"]
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        return False
    now = time.time() if now is None else now
    if timestamp > now or now - timestamp > max_age or not isinstance(signature, str) or received_binding != binding or healthy is not True:
        return False
    return hmac.compare_digest(signature, watchdog_signature(token, timestamp, binding, healthy))


class VictronGX(ComponentBase):
    """Read GX state and control ESS Mode 2 over local dbus-flashmq MQTT."""

    def initialize(self, config=None, **kwargs):
        """Validate explicit GX, device-instance, grid, limit, and watchdog configuration."""
        self.config = dict(config or {})
        self.host = self.config.get("host")
        if not isinstance(self.host, str) or not self.host.strip() or any(char in self.host for char in "/@"):
            raise ValueError("victron_gx.host must be a local MQTT hostname or IP address")
        self.host = self.host.strip()
        self.portal_id = self.config.get("portal_id")
        if not isinstance(self.portal_id, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", self.portal_id):
            raise ValueError("victron_gx.portal_id must be the GX VRM Portal ID")

        self.hub4_instance = _instance(self.config.get("hub4_instance"), "victron_gx.hub4_instance")
        self.vebus_instance = _instance(self.config.get("vebus_instance"), "victron_gx.vebus_instance")
        self.settings_instance = _instance(self.config.get("settings_instance"), "victron_gx.settings_instance")
        self.system_instance = _instance(self.config.get("system_instance"), "victron_gx.system_instance")
        self.battery_instance = _instance(self.config.get("battery_instance"), "victron_gx.battery_instance")
        self.grid_input = self.config.get("grid_input")
        if isinstance(self.grid_input, bool) or self.grid_input not in (0, 1):
            raise ValueError("victron_gx.grid_input must be 0 (AC input 1) or 1 (AC input 2)")
        self.dess_off_value = self.config.get("dess_off_value")
        if isinstance(self.dess_off_value, bool) or not isinstance(self.dess_off_value, int):
            raise ValueError("victron_gx.dess_off_value must be the observed numeric value for DESS off")

        self.charge_setpoint_w = _positive_number(self.config.get("charge_setpoint_w"), "victron_gx.charge_setpoint_w")
        self.export_setpoint_w = _positive_number(self.config.get("export_setpoint_w"), "victron_gx.export_setpoint_w")
        self.max_grid_import_w = _positive_number(self.config.get("max_grid_import_w"), "victron_gx.max_grid_import_w")
        self.max_grid_export_w = _positive_number(self.config.get("max_grid_export_w"), "victron_gx.max_grid_export_w")
        self.max_charge_current_a = _positive_number(self.config.get("max_charge_current_a"), "victron_gx.max_charge_current_a")
        self.max_discharge_power_w = _positive_number(self.config.get("max_discharge_power_w"), "victron_gx.max_discharge_power_w")
        self.watchdog_safe_charge_current_a = _nonnegative_number(self.config.get("watchdog_safe_max_charge_current_a"), "victron_gx.watchdog_safe_max_charge_current_a")
        safe_discharge_power = _nonnegative_number(self.config.get("watchdog_safe_max_discharge_power_w"), "victron_gx.watchdog_safe_max_discharge_power_w")
        if not safe_discharge_power.is_integer():
            raise ValueError("victron_gx.watchdog_safe_max_discharge_power_w must be a whole number")
        self.watchdog_safe_discharge_power_w = int(safe_discharge_power)
        if self.charge_setpoint_w > self.max_grid_import_w or self.export_setpoint_w > self.max_grid_export_w:
            raise ValueError("Victron requested setpoints must not exceed the explicit grid power caps")
        if self.watchdog_safe_charge_current_a > self.max_charge_current_a or self.watchdog_safe_discharge_power_w > self.max_discharge_power_w:
            raise ValueError("Victron watchdog fallback values must not exceed the configured charge and inverter caps")
        self.charge_setpoint_w = int(round(self.charge_setpoint_w))
        self.export_setpoint_w = int(round(self.export_setpoint_w))
        self.max_grid_import_w = int(round(self.max_grid_import_w))
        self.max_grid_export_w = int(round(self.max_grid_export_w))
        self.max_discharge_power_w = int(round(self.max_discharge_power_w))

        self.mqtt_port = int(self.config.get("port", VICTRON_MQTT_PORT))
        if not 1 <= self.mqtt_port <= 65535:
            raise ValueError("victron_gx.port must be between 1 and 65535")
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.tls_enabled = self.config.get("tls", True)
        if not isinstance(self.tls_enabled, bool):
            raise ValueError("victron_gx.tls must be true or false")
        self.ca_file = self.config.get("ca_file")
        self.control_enabled = self.config.get("enable_controls", False) is True
        self.watchdog_token = self.config.get("watchdog_token")
        self.watchdog_enabled = self.config.get("watchdog_required", False) is True
        if self.control_enabled and (not self.watchdog_enabled or not isinstance(self.watchdog_token, str) or len(self.watchdog_token) < 32):
            raise ValueError("Victron controls require watchdog_required: true and a watchdog_token of at least 32 characters")
        if not self.tls_enabled:
            self.log("Warn: Victron GX MQTT TLS is disabled; only use plaintext MQTT on a trusted isolated LAN")

        self.battery_service_type = self.config.get("battery_service_type")
        if self.battery_service_type not in ("battery", "vebus"):
            raise ValueError("victron_gx.battery_service_type must be explicitly battery or vebus")
        self.client = None
        self._mqtt_task = None
        self._connected = False
        self._full_publish_complete = False
        self._latest = {}
        self._pending = {}
        self._read_paths = self._build_read_paths()
        self._write_paths = {
            "setpoint": self._topic("hub4", self.hub4_instance, "Overrides/Setpoint"),
            "max_charge_current": self._topic("settings", self.settings_instance, "Settings/SystemSetup/MaxChargeCurrent"),
            "max_discharge_power": self._topic("settings", self.settings_instance, "Settings/CGwacs/MaxDischargePower"),
        }
        self.heartbeat_topic = "predbat/{}/victron/{}/heartbeat".format(self.base.prefix, self.portal_id)
        self.watchdog_topic = self.heartbeat_topic + "/watchdog"
        self.heartbeat_binding = {
            "portal_id": self.portal_id,
            "host": self.host,
            "port": self.mqtt_port,
            "hub4_instance": self.hub4_instance,
            "settings_instance": self.settings_instance,
            "heartbeat_topic": self.heartbeat_topic,
            "safe_max_charge_current_a": self.watchdog_safe_charge_current_a,
            "safe_max_discharge_power_w": self.watchdog_safe_discharge_power_w,
        }
        self._watchdog_alive_monotonic = None
        self.last_control_monotonic = None
        self._last_sensor_values = {}
        self._pending_operation = None
        self._cycle_rate_results = {}
        self._cycle_expected_rates = {}

    def _topic(self, service, instance, path, operation="N"):
        """Construct one dbus-flashmq topic from the explicit VRM ID and instance."""
        return "{}/{}/{}/{}/{}".format(operation, self.portal_id, service, instance, path)

    def _keepalive_topic(self):
        """Return the dbus-flashmq read topic that refreshes all live D-Bus values."""
        return "R/{}/keepalive".format(self.portal_id)

    def _build_read_paths(self):
        """Build the exact safety and limit topics used by this configured system."""
        vebus = self.vebus_instance
        settings = self.settings_instance
        system = self.system_instance
        battery = self.battery_instance
        paths = {
            "active_input": self._topic("vebus", vebus, "Ac/ActiveIn/ActiveInput"),
            "active_source": self._topic("system", system, "Ac/ActiveIn/Source"),
            "hub4_mode": self._topic("settings", settings, "Settings/CGwacs/Hub4Mode"),
            "dess_mode": self._topic("settings", settings, "Settings/DynamicEss/Mode"),
            "battery_soc": self._topic(self.battery_service_type, battery, "Soc"),
            "battery_voltage": self._topic(self.battery_service_type, battery, "Dc/0/Voltage"),
            "max_charge_current": self._topic("settings", settings, "Settings/SystemSetup/MaxChargeCurrent"),
            "max_discharge_power": self._topic("settings", settings, "Settings/CGwacs/MaxDischargePower"),
            "setpoint": self._topic("hub4", self.hub4_instance, "Overrides/Setpoint"),
        }
        if self.battery_service_type == "battery":
            paths.update(
                {
                    "allow_charge": self._topic("battery", battery, "Io/AllowToCharge"),
                    "allow_discharge": self._topic("battery", battery, "Io/AllowToDischarge"),
                    "bms_max_charge_current": self._topic("battery", battery, "Info/MaxChargeCurrent"),
                    "bms_max_discharge_current": self._topic("battery", battery, "Info/MaxDischargeCurrent"),
                }
            )
        else:
            paths.update(
                {
                    "allow_charge": self._topic("vebus", vebus, "Bms/AllowToCharge"),
                    "allow_discharge": self._topic("vebus", vebus, "Bms/AllowToDischarge"),
                    "bms_expected": self._topic("vebus", vebus, "Bms/BmsExpected"),
                }
            )
        return paths

    def _mqtt_options(self):
        """Build secure local broker connection options without disabling TLS verification."""
        options = {
            "hostname": self.host,
            "port": self.mqtt_port,
            "identifier": "predbat-victron-{}".format(self.base.prefix),
            "keepalive": VICTRON_MQTT_KEEPALIVE_SECONDS,
        }
        if self.username:
            options["username"] = self.username
        if self.password:
            options["password"] = self.password
        if self.tls_enabled:
            options["tls_context"] = ssl.create_default_context(cafile=self.ca_file)
        return options

    async def run(self, seconds, first):
        """Start and supervise the reconnecting MQTT task."""
        if aiomqtt is None and getattr(self.base, "mqtt_client_factory", None) is None:
            raise ImportError("Victron GX MQTT requires aiomqtt")
        if self._mqtt_task is None or self._mqtt_task.done():
            self._mqtt_task = asyncio.create_task(self._mqtt_loop())
        return True

    async def _mqtt_loop(self):
        """Keep one non-retained GX MQTT session alive and restart it after disconnects."""
        delay = 1
        while not self.api_stop:
            try:
                async with self.mqtt_client(**self._mqtt_options()) as client:
                    self.client = client
                    self._connected = True
                    self._full_publish_complete = False
                    self._watchdog_alive_monotonic = None
                    self._latest.clear()
                    await client.subscribe("N/{}/#".format(self.portal_id), qos=0)
                    if self.control_enabled:
                        await client.subscribe(self.watchdog_topic, qos=0)
                    nonce = secrets.token_urlsafe(18)
                    await client.publish(
                        self._keepalive_topic(),
                        json.dumps({"keepalive-options": [{"full-publish-completed-echo": nonce}]}),
                        qos=0,
                        retain=False,
                    )
                    self._initial_nonce = nonce
                    self.log("Victron GX MQTT connected to {}:{}".format(self.host, self.mqtt_port))
                    delay = 1
                    await self._connected_loop(client)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.log("Warn: Victron GX MQTT disconnected: {}".format(error))
                self.count_errors += 1
            finally:
                self._connected = False
                self._full_publish_complete = False
                self.client = None
                self._fail_pending()
            if not self.api_stop:
                await asyncio.sleep(delay)
                delay = min(delay * 2, VICTRON_RECONNECT_MAX_SECONDS)

    async def _connected_loop(self, client):
        """Run the notification listener and keepalive/read refresh together."""
        receiver = asyncio.create_task(self._receive(client))
        refresher = asyncio.create_task(self._refresh(client))
        try:
            done, _ = await asyncio.wait((receiver, refresher), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exception = task.exception()
                if exception:
                    raise exception
            raise ConnectionError("Victron MQTT listener stopped")
        finally:
            tasks = (receiver, refresher)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _receive(self, client):
        """Consume non-retained N notifications and resolve only matching write notifications."""
        async for message in client.messages:
            topic = str(message.topic)
            if topic == self.watchdog_topic and valid_watchdog_heartbeat(message.payload, self.watchdog_token, self.heartbeat_binding):
                self._watchdog_alive_monotonic = time.monotonic()
                continue
            parts = topic.split("/")
            if len(parts) < 3 or parts[0] != "N" or parts[1] != self.portal_id:
                continue
            if parts[2] == "full_publish_completed":
                self._handle_full_publish(message.payload)
                continue
            reverse = {"/".join(value.split("/")[2:]): key for key, value in self._read_paths.items()}
            name = reverse.get("/".join(parts[2:]))
            if name is None:
                continue
            value = _mqtt_value(message.payload)
            now = time.monotonic()
            self._latest[name] = {"value": value, "at": now}
            self._publish_telemetry(name, None if value is MQTT_INVALID else value)
            pending = self._pending.get(name)
            if pending and now > pending["issued"] and self._matches(name, value, pending["value"]):
                if not pending["future"].done():
                    pending["future"].set_result(True)
            if name in ("max_charge_current", "max_discharge_power"):
                self._publish_control_sensor(name, None if value is MQTT_INVALID else value)

    def _handle_full_publish(self, payload):
        """Mark initial state ready only after the echoed dbus-flashmq snapshot completes."""
        try:
            data = json.loads(payload)
        except (TypeError, ValueError, UnicodeDecodeError):
            return
        if data.get("full-publish-completed-echo") == getattr(self, "_initial_nonce", None):
            self._full_publish_complete = True
            self.log("Victron GX MQTT initial non-retained snapshot received")

    async def _refresh(self, client):
        """Maintain the Venus MQTT keepalive and reread interlock values before they go stale."""
        first = True
        while not self.api_stop:
            if first:
                first = False
            else:
                await client.publish(
                    self._keepalive_topic(),
                    json.dumps({"keepalive-options": ["suppress-republish"]}),
                    qos=0,
                    retain=False,
                )
            for topic in self._read_paths.values():
                await client.publish(topic.replace("N/", "R/", 1), b"", qos=0, retain=False)
            await asyncio.sleep(VICTRON_READ_REFRESH_SECONDS)

    def _publish_telemetry(self, name, value):
        """Publish selected MQTT readings as Predbat sensors for diagnostics."""
        sensor_map = {
            "battery_soc": ("victron_soc", "%"),
            "battery_voltage": ("victron_battery_voltage", "V"),
            "active_input": ("victron_active_input", None),
            "active_source": ("victron_active_source", None),
            "hub4_mode": ("victron_hub4_mode", None),
            "dess_mode": ("victron_dess_mode", None),
        }
        entity = sensor_map.get(name)
        if entity:
            self.base.set_state_wrapper("sensor.{}_{}".format(self.prefix, entity[0]), state=value if value is not None else "unavailable", attributes={"unit_of_measurement": entity[1]} if entity[1] else {})
        if name == "battery_voltage" and self._fresh("max_charge_current") is not None:
            self._publish_control_sensor("max_charge_current", self._fresh("max_charge_current"))

    def _publish_control_sensor(self, name, value):
        """Publish verified control values as power figures used by the standard inverter reader."""
        if name == "max_charge_current":
            voltage = self._fresh("battery_voltage")
            entity = "sensor.{}_victron_charge_rate".format(self.prefix)
            if not _is_finite_number(value) or not _is_finite_number(voltage):
                self.base.set_state_wrapper(entity, state="unavailable", attributes={"unit_of_measurement": "W"})
                return
            value = float(value) * float(voltage)
        else:
            entity = "sensor.{}_victron_discharge_rate".format(self.prefix)
        if value is None or not _is_finite_number(value):
            self.base.set_state_wrapper(entity, state="unavailable", attributes={"unit_of_measurement": "W"})
            return
        self.base.set_state_wrapper(entity, state=round(float(value), 1), attributes={"unit_of_measurement": "W", "state_class": "measurement"})

    def _fresh(self, name, now=None):
        """Return a telemetry value only while its notification is within the freshness bound."""
        value = self._fresh_value(name, now)
        return None if value is MQTT_MISSING else value

    def _fresh_value(self, name, now=None):
        """Return a fresh value while preserving valid JSON null separately from missing data."""
        item = self._latest.get(name)
        if not item:
            return MQTT_MISSING
        now = time.monotonic() if now is None else now
        if now - item["at"] > VICTRON_CONTROL_STALE_SECONDS or item["value"] is MQTT_INVALID:
            return MQTT_MISSING
        return item["value"]

    def _control_ready(self, now=None):
        """Require fresh Hub4Mode 2, DESS off, and a confirmed grid input for Mode 2 control."""
        if not self._connected or not self._full_publish_complete:
            return False
        if self.control_enabled:
            now = time.monotonic() if now is None else now
            if self._watchdog_alive_monotonic is None or now - self._watchdog_alive_monotonic > VICTRON_WATCHDOG_STALE_SECONDS:
                return False
        required = [
            "active_input",
            "active_source",
            "hub4_mode",
            "dess_mode",
            "allow_charge",
            "allow_discharge",
            "battery_soc",
            "battery_voltage",
            "max_charge_current",
            "max_discharge_power",
        ]
        if self.battery_service_type == "battery":
            required.extend(("bms_max_charge_current", "bms_max_discharge_current"))
        else:
            required.append("bms_expected")
        values = {name: self._fresh(name, now) for name in required}
        if any(value is None for value in values.values()):
            return False
        setpoint = self._fresh_value("setpoint", now)
        if setpoint is MQTT_MISSING:
            return False
        if setpoint is not None and (isinstance(setpoint, bool) or not _is_finite_number(setpoint)):
            return False
        try:
            for name in required:
                if not _is_finite_number(values[name]):
                    return False
            if self.battery_service_type == "battery" and (float(values["bms_max_charge_current"]) < 0 or float(values["bms_max_discharge_current"]) < 0):
                return False
            if self.battery_service_type == "vebus" and float(values["bms_expected"]) != 1:
                return False
            if float(values["hub4_mode"]) != 2 or float(values["dess_mode"]) != self.dess_off_value:
                return False
            if float(values["active_input"]) == VICTRON_DISCONNECTED_INPUT or float(values["active_input"]) != self.grid_input:
                return False
            if float(values["active_source"]) != VICTRON_GRID_SOURCE:
                return False
            if float(values["allow_charge"]) not in (0.0, 1.0) or float(values["allow_discharge"]) not in (0.0, 1.0):
                return False
            if float(values["battery_voltage"]) <= 0 or not 0 <= float(values["battery_soc"]) <= 100:
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        return True

    def _bms_current_limit(self, direction):
        """Return the appropriate current ceiling for the explicitly selected BMS topology."""
        if self.battery_service_type == "battery":
            return self._fresh("bms_max_{}_current".format(direction))
        return self.max_charge_current_a if direction == "charge" else self.max_discharge_power_w / max(float(self._fresh("battery_voltage") or 0), 1)

    def _matches(self, name, current, expected):
        """Compare a received D-Bus value using that setting's supported precision."""
        if name == "setpoint" and expected is None:
            return current is None
        if current is MQTT_MISSING or current is MQTT_INVALID:
            return False
        return _within(current, expected, 1.0 if name in ("setpoint", "max_discharge_power") else 0.11)

    async def _write_verified(self, name, value):
        """Publish one non-retained W request and wait for a later matching N notification."""
        clearing_setpoint = name == "setpoint" and value is None
        if clearing_setpoint:
            ready = self._connected and self._full_publish_complete
        else:
            ready = self._control_ready()
        if not ready:
            self.log("Warn: Victron GX control interlock is not ready; refusing {}".format(name))
            return False
        current = self._fresh_value(name) if name == "setpoint" else self._fresh(name)
        if self._matches(name, current, value):
            return True
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        pending = {"value": value, "issued": time.monotonic(), "future": future}
        self._pending[name] = pending
        topic = self._write_paths[name].replace("N/", "W/", 1)
        try:
            await self.client.publish(topic, json.dumps({"value": value}), qos=0, retain=False)
            return await asyncio.wait_for(future, timeout=VICTRON_WRITE_ACK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            self.log("Warn: Victron GX {} write did not receive a matching N notification".format(name))
            return False
        except Exception as error:
            self.log("Warn: Victron GX {} write failed: {}".format(name, error))
            return False
        finally:
            if self._pending.get(name) is pending:
                self._pending.pop(name, None)

    @writes
    async def set_charge_power_w(self, power_w):
        """Set the DVCC system charge-current cap from a requested battery power in watts."""
        if not self.control_enabled or not self._control_ready():
            return False
        if self.base.set_read_only:
            return False
        try:
            request = max(0.0, min(float(power_w), self.max_charge_current_a * 1000.0))
        except (TypeError, ValueError):
            return False
        voltage = float(self._fresh("battery_voltage"))
        bms_current = float(self._bms_current_limit("charge"))
        allowed = int(self._fresh("allow_charge")) == 1
        if voltage <= 0 or bms_current < 0:
            return False
        amps = 0.0 if request == 0 else min(request / voltage, self.max_charge_current_a, bms_current)
        if amps > 0 and not allowed:
            self.log("Warn: Victron BMS disallows charging; refusing a positive charge current")
            return False
        amps = round(amps, 1)
        result = await self._write_verified("max_charge_current", amps)
        if self._cycle_active:
            self._cycle_rate_results["charge"] = result
            if result:
                self._cycle_expected_rates["max_charge_current"] = amps
        return result

    @writes
    async def set_discharge_power_w(self, power_w):
        """Set the persistent ESS maximum inverter output power in watts."""
        if not self.control_enabled or not self._control_ready():
            return False
        if self.base.set_read_only:
            return False
        try:
            request = max(0.0, min(float(power_w), self.max_discharge_power_w))
        except (TypeError, ValueError):
            return False
        voltage = float(self._fresh("battery_voltage"))
        bms_current = float(self._bms_current_limit("discharge"))
        allowed = int(self._fresh("allow_discharge")) == 1
        if voltage <= 0 or bms_current < 0:
            return False
        request = min(request, voltage * bms_current)
        if request > 0 and not allowed:
            self.log("Warn: Victron BMS disallows discharging; refusing a positive inverter power limit")
            return False
        request = int(round(request))
        result = await self._write_verified("max_discharge_power", request)
        if self._cycle_active:
            self._cycle_rate_results["discharge"] = result
            if result:
                self._cycle_expected_rates["max_discharge_power"] = request
        return result

    @writes
    async def set_operation(self, target_soc, charging, exporting):
        """Stage intentional non-read-only modes and queue active setpoints until rates verify."""
        self._pending_operation = None
        self._cycle_active = False
        self._cycle_rate_results = {}
        self._cycle_expected_rates = {}
        if not self.control_enabled:
            return False
        if self.base.set_read_only or (bool(charging) and bool(exporting)):
            return await self._write_verified("setpoint", None)
        if not charging and not exporting:
            self._cycle_active = True
            result = await self._write_verified("setpoint", None)
            if not result:
                self._cycle_active = False
            return result
        if not self._control_ready():
            return False
        self._cycle_active = True
        current = self._fresh_value("setpoint")
        desired = victron_setpoint(
            target_soc,
            self._fresh("battery_soc"),
            charging,
            exporting,
            min(self.charge_setpoint_w, self.max_grid_import_w),
            min(self.export_setpoint_w, self.max_grid_export_w),
        )
        rejected = False
        if desired is not None and desired > 0 and (int(self._fresh("allow_charge")) != 1 or float(self._bms_current_limit("charge")) <= 0):
            self.log("Warn: Victron BMS disallows charging; not requesting grid import")
            desired = None
            rejected = True
        if desired is not None and desired < 0 and (int(self._fresh("allow_discharge")) != 1 or float(self._bms_current_limit("discharge")) <= 0):
            self.log("Warn: Victron BMS disallows discharging; not requesting export")
            desired = None
            rejected = True
        if desired is None:
            result = await self._write_verified("setpoint", None)
            if rejected or not result:
                self._cycle_active = False
            return result
        if current is not None and not await self._write_verified("setpoint", None):
            self._cycle_active = False
            return False
        self._pending_operation = desired
        return True

    def _transition_telemetry_matches(self, desired):
        """Check all mode inputs and rate limits together after the final setpoint notification."""
        if not self._control_ready():
            return False
        current_setpoint = self._fresh_value("setpoint")
        if not self._matches("setpoint", current_setpoint, desired):
            return False
        for name, expected in self._cycle_expected_rates.items():
            if not self._matches(name, self._fresh(name), expected):
                return False
        charge_limit = self._cycle_expected_rates.get("max_charge_current")
        discharge_limit = self._cycle_expected_rates.get("max_discharge_power")
        if charge_limit is None or discharge_limit is None:
            return False
        if charge_limit > 0 and int(self._fresh("allow_charge")) != 1:
            return False
        if discharge_limit > 0 and int(self._fresh("allow_discharge")) != 1:
            return False
        if self.battery_service_type == "battery":
            if charge_limit > float(self._fresh("bms_max_charge_current")):
                return False
            if discharge_limit > float(self._fresh("bms_max_discharge_current")) * float(self._fresh("battery_voltage")):
                return False
        return True

    @writes
    async def finalize_control_cycle(self):
        """Verify intentional modes and commit active setpoints only after both rates are acknowledged."""
        if not self._cycle_active:
            return True
        desired = self._pending_operation
        self._pending_operation = None
        if self._cycle_rate_results != {"charge": True, "discharge": True} or not self._control_ready():
            self.log("Warn: Victron GX mode transition aborted because both rate limits were not verified")
            await self._write_verified("setpoint", None)
            self._cycle_active = False
            self._cycle_rate_results = {}
            self._cycle_expected_rates = {}
            return False
        if desired is not None and not await self._write_verified("setpoint", desired):
            await self._write_verified("setpoint", None)
            self._cycle_active = False
            self._cycle_rate_results = {}
            self._cycle_expected_rates = {}
            return False
        if not self._transition_telemetry_matches(desired):
            self.log("Warn: Victron GX mode transition did not pass its combined post-write verification")
            await self._write_verified("setpoint", None)
            self._cycle_active = False
            self._cycle_rate_results = {}
            self._cycle_expected_rates = {}
            return False
        await self._publish_heartbeat()
        self.last_control_monotonic = time.monotonic()
        self._cycle_active = False
        self._cycle_rate_results = {}
        self._cycle_expected_rates = {}
        return True

    async def _publish_heartbeat(self):
        """Publish a signed, non-retained heartbeat for the separately run watchdog."""
        timestamp = int(time.time())
        payload = json.dumps(
            {
                "timestamp": timestamp,
                "binding": self.heartbeat_binding,
                "healthy": True,
                "signature": watchdog_signature(self.watchdog_token, timestamp, self.heartbeat_binding, True),
            }
        )
        await self.client.publish(self.heartbeat_topic, payload, qos=0, retain=False)

    def _sync_control(self, coroutine, operation):
        """Run a verified async control on the component loop from Predbat's synchronous executor."""
        loop = self.component_loop
        if loop is None or loop.is_closed() or not loop.is_running():
            coroutine.close()
            self.log("Warn: Victron GX {} refused because its MQTT component loop is not running".format(operation))
            return False
        try:
            if asyncio.get_running_loop() is loop:
                coroutine.close()
                self.log("Warn: Victron GX {} refused from its own event loop".format(operation))
                return False
        except RuntimeError:
            pass
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        try:
            return future.result(timeout=VICTRON_WRITE_ACK_TIMEOUT_SECONDS + 2)
        except Exception as error:
            future.cancel()
            self.log("Warn: Victron GX {} failed: {}".format(operation, error))
            return False

    def set_charge_power_sync(self, power_w):
        """Synchronously set and verify the configured DVCC charge-current limit."""
        return self._sync_control(self.set_charge_power_w(power_w), "charge current")

    def set_discharge_power_sync(self, power_w):
        """Synchronously set and verify the configured maximum inverter power."""
        return self._sync_control(self.set_discharge_power_w(power_w), "maximum inverter power")

    def set_operation_sync(self, target_soc, charging, exporting):
        """Synchronously validate and queue the Mode 2 grid setpoint."""
        return self._sync_control(self.set_operation(target_soc, charging, exporting), "Mode 2 setpoint")

    def finalize_control_cycle_sync(self):
        """Synchronously commit a queued Mode 2 setpoint after the executor's rate pass."""
        return self._sync_control(self.finalize_control_cycle(), "complete Mode 2 transition")

    def clear_override_sync(self):
        """Synchronously clear the volatile setpoint without requiring active-mode telemetry."""
        return self._sync_control(self.set_operation(100, False, False), "clear Mode 2 override")

    def has_pending_operation(self):
        """Whether this executor cycle queued a validated directional setpoint."""
        return self._pending_operation is not None

    def _fail_pending(self):
        """Fail unresolved writes when the GX MQTT connection ends."""
        for pending in list(self._pending.values()):
            future = pending.get("future")
            if future and not future.done():
                future.set_result(False)
        self._pending.clear()

    async def final(self):
        """Stop the MQTT listener without writing persistent controls during shutdown."""
        if self._mqtt_task and not self._mqtt_task.done():
            self._mqtt_task.cancel()
            try:
                await self._mqtt_task
            except asyncio.CancelledError:
                pass
        self._fail_pending()
