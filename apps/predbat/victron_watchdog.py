# -----------------------------------------------------------------------------
# Predbat Home Battery System
# Copyright Trefor Southwell 2026 - All Rights Reserved
# This application maybe used for personal use only and not for commercial use
# -----------------------------------------------------------------------------
# fmt off
# pylint: disable=consider-using-f-string
# pylint: disable=line-too-long

"""Independent GX MQTT watchdog for Victron controls written by Predbat."""

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import ssl
import time

try:
    import aiomqtt
except ImportError:
    aiomqtt = None

WATCHDOG_DEFAULT_TIMEOUT_SECONDS = 900
WATCHDOG_ACK_TIMEOUT_SECONDS = 8
WATCHDOG_RECONNECT_MAX_SECONDS = 60
WATCHDOG_HEARTBEAT_SECONDS = 20
WATCHDOG_FALLBACK_RETRY_SECONDS = 30
WATCHDOG_INVALID = object()


def _signature(token, timestamp, binding, healthy=True):
    """Sign a timestamp, heartbeat health, and exact GX/watchdog configuration."""
    data = json.dumps({"timestamp": int(timestamp), "binding": binding, "healthy": bool(healthy)}, sort_keys=True, separators=(",", ":"))
    return hmac.new(token.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def _valid_heartbeat(payload, token, binding, now=None, max_age=120):
    """Validate a recent Predbat heartbeat and its exact GX/watchdog configuration."""
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
    return hmac.compare_digest(signature, _signature(token, timestamp, binding, healthy))


class VictronWatchdog:
    """Reconnect to a local GX and clear stale Predbat commands independently."""

    def __init__(self, config, mqtt_client_factory=None, monotonic=None, wall_time=None):
        """Validate explicit connection, topic, credentials, and safe fallback values."""
        self.config = dict(config)
        self.mqtt_client_factory = mqtt_client_factory
        self.monotonic = monotonic or time.monotonic
        self.wall_time = wall_time or time.time
        self.host = self.config["host"]
        self.portal_id = self.config["portal_id"]
        self.hub4_instance = int(self.config["hub4_instance"])
        self.settings_instance = int(self.config["settings_instance"])
        self.port = int(self.config.get("port", 8883))
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.tls = self.config.get("tls", True)
        self.ca_file = self.config.get("ca_file")
        self.token = self.config["token"]
        self.heartbeat_topic = self.config["heartbeat_topic"]
        self.timeout = int(self.config.get("timeout_seconds", WATCHDOG_DEFAULT_TIMEOUT_SECONDS))
        self.safe_charge_current_a = float(self.config["safe_max_charge_current_a"])
        self.safe_discharge_power_w = int(self.config["safe_max_discharge_power_w"])
        self.started_at = self.monotonic()
        self.logger = logging.getLogger("victron_watchdog")
        if self.timeout < 30:
            raise ValueError("timeout_seconds must be at least 30")
        if not math.isfinite(self.safe_charge_current_a) or self.safe_charge_current_a < 0 or self.safe_discharge_power_w < 0:
            raise ValueError("safe fallback current and power must not be negative")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        self.latest = {}
        self.pending = {}
        self.last_heartbeat = None
        self.connected_at = self.started_at
        self.last_fallback = None
        self.fallback_task = None
        self.client = None
        self.healthy = False
        self.fallback_failed = False
        self.initial_fallback_pending = True
        self.watchdog_topic = self.heartbeat_topic + "/watchdog"

    def _topic(self, operation, service, instance, path):
        """Build a dbus-flashmq topic for one explicitly configured instance."""
        return "{}/{}/{}/{}/{}".format(operation, self.portal_id, service, instance, path)

    def _options(self):
        """Create an MQTT client configuration with standard TLS certificate validation."""
        options = {
            "hostname": self.host,
            "port": self.port,
            "identifier": "predbat-victron-watchdog",
            "keepalive": 30,
        }
        if self.username:
            options["username"] = self.username
        if self.password:
            options["password"] = self.password
        if self.tls:
            options["tls_context"] = ssl.create_default_context(cafile=self.ca_file)
        return options

    def _paths(self):
        """Return the only GX settings the independent watchdog is allowed to change."""
        return {
            "setpoint": ("hub4", self.hub4_instance, "Overrides/Setpoint", None),
            "max_charge_current": ("settings", self.settings_instance, "Settings/SystemSetup/MaxChargeCurrent", self.safe_charge_current_a),
            "max_discharge_power": ("settings", self.settings_instance, "Settings/CGwacs/MaxDischargePower", self.safe_discharge_power_w),
        }

    def _heartbeat_binding(self):
        """Bind signed heartbeats to the broker, GX and exact fallback controls."""
        return {
            "portal_id": self.portal_id,
            "host": self.host,
            "port": self.port,
            "hub4_instance": self.hub4_instance,
            "settings_instance": self.settings_instance,
            "heartbeat_topic": self.heartbeat_topic,
            "safe_max_charge_current_a": self.safe_charge_current_a,
            "safe_max_discharge_power_w": self.safe_discharge_power_w,
        }

    async def run(self):
        """Run continuously, falling back after the configured heartbeat timeout."""
        if aiomqtt is None and self.mqtt_client_factory is None:
            raise ImportError("The Victron GX watchdog requires aiomqtt")
        delay = 1
        while True:
            try:
                factory = self.mqtt_client_factory or aiomqtt.Client
                async with factory(**self._options()) as client:
                    self.client = client
                    self.latest.clear()
                    await self._connected(client)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger.warning("Victron watchdog disconnected: %s", error)
                self.client = None
                if self.fallback_task is not None and not self.fallback_task.done():
                    self.fallback_task.cancel()
                    try:
                        await self.fallback_task
                    except asyncio.CancelledError:
                        pass
                    self.fallback_failed = True
                    self.healthy = False
            await asyncio.sleep(delay)
            delay = min(delay * 2, WATCHDOG_RECONNECT_MAX_SECONDS)

    async def _connected(self, client):
        """Subscribe to the signed heartbeat and receive GX write notifications."""
        await client.subscribe("N/{}/#".format(self.portal_id), qos=0)
        await client.subscribe(self.heartbeat_topic, qos=0)
        await client.subscribe(self.watchdog_topic, qos=0)
        nonce = "watchdog-" + str(int(self.wall_time()))
        await client.publish(
            "R/{}/keepalive".format(self.portal_id),
            json.dumps({"keepalive-options": [{"full-publish-completed-echo": nonce}]}),
            qos=0,
            retain=False,
        )
        for service, instance, path, _value in self._paths().values():
            await client.publish(self._topic("R", service, instance, path), b"", qos=0, retain=False)
        if self.initial_fallback_pending:
            self.initial_fallback_pending = False
            self.last_fallback = self.monotonic()
            self.logger.warning("Watchdog started; applying configured fallback before certifying healthy")
            self.fallback_task = asyncio.create_task(self._apply_fallback())

        message_task = asyncio.create_task(client.messages.__anext__())
        keepalive_at = self.monotonic() + 30
        watchdog_heartbeat_at = self.monotonic()
        try:
            while True:
                done, _ = await asyncio.wait((message_task,), timeout=1)
                if done:
                    message = message_task.result()
                    self._handle_message(message)
                    message_task = asyncio.create_task(client.messages.__anext__())
                now = self.monotonic()
                if now >= watchdog_heartbeat_at:
                    timestamp = int(self.wall_time())
                    binding = self._heartbeat_binding()
                    heartbeat = {
                        "timestamp": timestamp,
                        "binding": binding,
                        "healthy": self.healthy,
                        "signature": _signature(self.token, timestamp, binding, self.healthy),
                    }
                    await client.publish(self.watchdog_topic, json.dumps(heartbeat), qos=0, retain=False)
                    watchdog_heartbeat_at = now + WATCHDOG_HEARTBEAT_SECONDS
                if now >= keepalive_at:
                    await client.publish(
                        "R/{}/keepalive".format(self.portal_id),
                        json.dumps({"keepalive-options": ["suppress-republish"]}),
                        qos=0,
                        retain=False,
                    )
                    for service, instance, path, _value in self._paths().values():
                        await client.publish(self._topic("R", service, instance, path), b"", qos=0, retain=False)
                    keepalive_at = now + 30
                last_seen = self.last_heartbeat if self.last_heartbeat is not None else self.connected_at
                fallback_running = self.fallback_task is not None and not self.fallback_task.done()
                fallback_due = self.last_fallback is None or now - self.last_fallback >= WATCHDOG_FALLBACK_RETRY_SECONDS
                timeout_expired = now - last_seen >= self.timeout
                if (self.fallback_failed and not fallback_running and fallback_due) or (timeout_expired and not fallback_running and fallback_due):
                    self.last_fallback = now
                    self.healthy = False
                    self.logger.error("Predbat heartbeat expired; applying configured Victron fallback")
                    self.fallback_task = asyncio.create_task(self._apply_fallback())
                elif self.last_heartbeat is not None and self.last_fallback is not None and self.last_heartbeat >= self.last_fallback:
                    self.last_fallback = None
        finally:
            if not message_task.done():
                message_task.cancel()
                try:
                    await message_task
                except asyncio.CancelledError:
                    pass

    def _handle_message(self, message):
        """Accept only signed recent heartbeats and track W-request confirmations."""
        topic = str(message.topic)
        if topic == self.heartbeat_topic:
            if _valid_heartbeat(message.payload, self.token, self._heartbeat_binding(), now=self.wall_time()):
                self.last_heartbeat = self.monotonic()
                self.healthy = not self.fallback_failed
                if self.last_fallback is not None and self.last_heartbeat >= self.last_fallback:
                    self.last_fallback = None
                    if self.fallback_task is not None and not self.fallback_task.done():
                        self.fallback_task.cancel()
            return
        parts = topic.split("/")
        if len(parts) < 5 or parts[0] != "N" or parts[1] != self.portal_id:
            return
        key = "/".join(parts[2:])
        for name, (service, instance, path, _value) in self._paths().items():
            if key != "{}/{}/{}".format(service, instance, path):
                continue
            try:
                data = json.loads(message.payload)
                value = data["value"] if isinstance(data, dict) and "value" in data else WATCHDOG_INVALID
            except (TypeError, ValueError, UnicodeDecodeError, AttributeError):
                value = WATCHDOG_INVALID
            self.latest[name] = value
            pending = self.pending.get(name)
            if pending and _same(value, pending["value"]) and self.monotonic() > pending["issued"]:
                if not pending["future"].done():
                    pending["future"].set_result(True)
            break

    async def _apply_fallback(self):
        """Write and verify safe setpoint and explicit user-selected persistent limits."""
        fallback_started = self.last_fallback
        success = True
        for name, (service, instance, path, value) in self._paths().items():
            if self.last_heartbeat is not None and fallback_started is not None and self.last_heartbeat >= fallback_started:
                return False
            if not await self._write_verified(name, service, instance, path, value):
                success = False
                self.logger.error("Victron watchdog fallback for %s was not confirmed", name)
        if success:
            self.logger.info("Victron watchdog fallback values were confirmed by GX notifications")
        else:
            self.logger.error("Victron watchdog fallback is incomplete; retrying in %s seconds", WATCHDOG_FALLBACK_RETRY_SECONDS)
        self.fallback_failed = not success
        self.healthy = success
        return success

    async def _write_verified(self, name, service, instance, path, value):
        """Wait for a matching notification rather than treating publish completion as an ACK."""
        if name in self.latest and _same(self.latest[name], value):
            return True
        future = asyncio.get_running_loop().create_future()
        pending = {"value": value, "issued": self.monotonic(), "future": future}
        self.pending[name] = pending
        try:
            await self.client.publish(self._topic("W", service, instance, path), json.dumps({"value": value}), qos=0, retain=False)
            return await asyncio.wait_for(future, timeout=WATCHDOG_ACK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            self.logger.error("Victron watchdog %s write did not receive a matching GX notification", name)
            return False
        except Exception as error:
            self.logger.error("Victron watchdog %s write failed: %s", name, error)
            return False
        finally:
            if self.pending.get(name) is pending:
                self.pending.pop(name, None)


def _same(actual, expected):
    """Compare numeric GX values using their D-Bus setting precision."""
    if actual is WATCHDOG_INVALID:
        return False
    if expected is None:
        return actual is None
    try:
        return abs(float(actual) - float(expected)) <= 0.1
    except (TypeError, ValueError):
        return False


def watchdog_config_from_args(args):
    """Resolve non-secret CLI settings and secrets from protected environment variables."""
    password = os.environ.get(args.password_env) if args.password_env else None
    token = os.environ.get(args.token_env)
    if not token or len(token) < 32:
        raise ValueError("Set the shared watchdog token in the configured environment variable (minimum 32 characters)")
    return {
        "host": args.host,
        "portal_id": args.portal_id,
        "hub4_instance": args.hub4_instance,
        "settings_instance": args.settings_instance,
        "port": args.port,
        "username": args.username,
        "password": password,
        "tls": not args.insecure,
        "ca_file": args.ca_file,
        "heartbeat_topic": args.heartbeat_topic,
        "token": token,
        "timeout_seconds": args.timeout_seconds,
        "safe_max_charge_current_a": args.safe_max_charge_current_a,
        "safe_max_discharge_power_w": args.safe_max_discharge_power_w,
    }


def main():
    """Parse command-line configuration and run the external watchdog."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="Local GX MQTT broker hostname or IP")
    parser.add_argument("--portal-id", required=True)
    parser.add_argument("--hub4-instance", type=int, required=True)
    parser.add_argument("--settings-instance", type=int, required=True)
    parser.add_argument("--heartbeat-topic", required=True)
    parser.add_argument("--safe-max-charge-current-a", type=float, required=True)
    parser.add_argument("--safe-max-discharge-power-w", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=WATCHDOG_DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--port", type=int, default=8883)
    parser.add_argument("--username")
    parser.add_argument("--password-env", default="VICTRON_MQTT_PASSWORD")
    parser.add_argument("--token-env", default="VICTRON_WATCHDOG_TOKEN")
    parser.add_argument("--ca-file")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS only on an isolated trusted LAN")
    args = parser.parse_args()
    asyncio.run(VictronWatchdog(watchdog_config_from_args(args)).run())


if __name__ == "__main__":
    main()
