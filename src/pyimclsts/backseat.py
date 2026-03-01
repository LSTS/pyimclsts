"""Generic backseat primitives built on top of ``pyimclsts``.

This module provides a reusable base controller that covers four common needs:

1. IMC TCP lifecycle (connect, run, stop)
2. Generic mission finite state machine (FSM)
3. Communication channel handlers (IMC/SMS/Iridium extensible queues)
4. Lightweight web UI for start/stop/pause/status/logs

The design is intentionally small and dependency-free (stdlib only) so it can be
used as a foundation for different mission-specific backseats.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import queue
import threading
import time
import traceback
from typing import Any, Callable, Deque, Dict, List, Optional

import pyimclsts.network as _network

try:
    import pyimc_generated as _pg
except Exception:  # pragma: no cover - runtime dependency
    _pg = None


StateHandler = Callable[["BackseatContext"], Optional[str]]
CommHandler = Callable[[Callable[..., None], Any, Dict[str, Any]], None]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class _OutboundItem:
    channel: str
    payload: Any
    options: Dict[str, Any]


class BackseatContext:
    """Execution context passed to FSM state handlers."""

    __slots__ = ("_backseat", "_send")

    def __init__(self, backseat: "BaseBackseat", send_callback: Callable[..., None]) -> None:
        self._backseat = backseat
        self._send = send_callback

    @property
    def backseat(self) -> "BaseBackseat":
        return self._backseat

    @property
    def state_name(self) -> Optional[str]:
        return self._backseat.state_name

    @property
    def estimated_state(self) -> Any:
        return self._backseat.latest("EstimatedState")

    @property
    def follow_ref_state(self) -> Any:
        return self._backseat.latest("FollowRefState")

    @property
    def plan_control_state(self) -> Any:
        return self._backseat.latest("PlanControlState")

    def send_now(self, message: Any, **send_kwargs: Any) -> None:
        self._backseat._send_now(self._send, message, **send_kwargs)

    def queue(self, channel: str, payload: Any, **options: Any) -> None:
        self._backseat.queue_communication(channel, payload, **options)

    def goto_state(self, state_name: str) -> str:
        return state_name

    def log(self, text: str) -> None:
        self._backseat.log(text)


class BackseatWebUI:
    """Lightweight HTTP control surface for a backseat instance."""

    def __init__(self, backseat: "BaseBackseat", host: str = "0.0.0.0", port: int = 8091) -> None:
        self._backseat = backseat
        self._host = host
        self._port = port
        self._server = None  # type: Optional[ThreadingHTTPServer]
        self._thread = None  # type: Optional[threading.Thread]
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return
        self._server = ThreadingHTTPServer((self._host, self._port), self._build_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, name="backseat-web", daemon=True)
        self._thread.start()
        self._running = True
        self._backseat.log("Web UI started on http://%s:%s" % (self._host, self._port))

    def stop(self) -> None:
        if not self._running:
            return
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None
        self._running = False
        self._backseat.log("Web UI stopped")

    def _build_handler(self) -> type:
        backseat = self._backseat

        class Handler(BaseHTTPRequestHandler):
            server_version = "BackseatWebUI/1.0"

            def log_message(self, fmt: str, *args: Any) -> None:
                # Silence BaseHTTPRequestHandler's default logging.
                return

            def _read_json(self) -> Dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0:
                    return {}
                raw = self.rfile.read(length)
                if not raw:
                    return {}
                try:
                    return json.loads(raw.decode("utf-8"))
                except Exception:
                    return {}

            def _write_json(self, body: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
                data = json.dumps(body, indent=2, sort_keys=True).encode("utf-8")
                self.send_response(status.value)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _write_html(self, html: str, status: HTTPStatus = HTTPStatus.OK) -> None:
                data = html.encode("utf-8")
                self.send_response(status.value)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:  # noqa: N802 - stdlib interface
                if self.path == "/":
                    self._write_html(backseat._render_web_index())  # pylint: disable=protected-access
                    return
                if self.path == "/api/status":
                    self._write_json(backseat.status_snapshot())
                    return
                if self.path == "/api/logs":
                    self._write_json({"logs": backseat.logs()})
                    return

                self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:  # noqa: N802 - stdlib interface
                payload = self._read_json()

                if self.path == "/api/network/start":
                    backseat.start_network()
                    self._write_json({"ok": True})
                    return
                if self.path == "/api/network/stop":
                    backseat.stop_network()
                    self._write_json({"ok": True})
                    return
                if self.path == "/api/mission/start":
                    backseat.start_mission()
                    self._write_json({"ok": True})
                    return
                if self.path == "/api/mission/pause":
                    backseat.pause_mission()
                    self._write_json({"ok": True})
                    return
                if self.path == "/api/mission/resume":
                    backseat.resume_mission()
                    self._write_json({"ok": True})
                    return
                if self.path == "/api/mission/stop":
                    backseat.stop_mission()
                    self._write_json({"ok": True})
                    return
                if self.path == "/api/comm/send":
                    channel = str(payload.get("channel", "sms"))
                    message = payload.get("payload", "")
                    ttl = int(payload.get("ttl", 60))
                    destination = str(payload.get("destination", "broadcast"))
                    backseat.queue_communication(channel, message, ttl=ttl, destination=destination)
                    self._write_json({"ok": True})
                    return

                self._write_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

        return Handler


class BaseBackseat:
    """Generic backseat base controller.

    Typical usage:

    1. Subclass and register FSM states.
    2. Start network and optional web UI.
    3. Drive mission behavior through state handlers.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6006,
        target_system: Optional[str] = None,
        *,
        control_period: float = 1.0,
        comm_period: float = 1.0,
        big_endian: bool = False,
        use_mp: bool = False,
        log_limit: int = 500,
    ) -> None:
        if _pg is None:
            raise RuntimeError(
                "pyimc_generated could not be imported. "
                "Run `python -m pyimclsts.extract` in your working directory first."
            )

        self.host = host
        self.port = port
        self.target_system = target_system
        self.control_period = float(control_period)
        self.comm_period = float(comm_period)
        self.big_endian = big_endian
        self.use_mp = use_mp

        self._state_handlers = {}  # type: Dict[str, StateHandler]
        self._state_name = None  # type: Optional[str]

        self._mission_active = False
        self._paused = True
        self._finished = False

        self._lock = threading.RLock()
        self._peers = {}  # type: Dict[str, Dict[str, Any]]
        self._latest = {}  # type: Dict[str, Any]
        self._latest_ts = {}  # type: Dict[str, float]
        self._logs = deque(maxlen=log_limit)  # type: Deque[str]

        self._request_id = int(time.time()) & 0xFFFF
        self._thread = None  # type: Optional[threading.Thread]
        self._web_ui = None  # type: Optional[BackseatWebUI]

        self._command_queue = queue.Queue()  # type: queue.Queue
        self._outbound_queue = queue.Queue()  # type: queue.Queue

        conn = _network.tcp_interface(self.host, self.port)
        self._subscriber = _network.subscriber(conn, big_endian=self.big_endian, use_mp=self.use_mp)
        self._subscriber.periodic_async(self._control_tick, self.control_period)
        self._subscriber.periodic_async(self._communication_tick, self.comm_period)

        self._comm_handlers = {}  # type: Dict[str, CommHandler]
        self.register_comm_handler("imc", self._handle_imc)
        self.register_comm_handler("sms", self._handle_sms)
        self.register_comm_handler("iridium", self._handle_iridium)

        self._register_builtin_subscriptions()

    # -------------------------------------------------------------------------
    # Public lifecycle
    # -------------------------------------------------------------------------
    def start_network(self) -> None:
        if self.is_network_running:
            return
        self._thread = threading.Thread(target=self._run_subscriber, name="backseat-imc", daemon=True)
        self._thread.start()

    def stop_network(self, timeout: float = 3.0) -> None:
        self._subscriber.stop()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)

    def start_web_ui(self, host: str = "0.0.0.0", port: int = 8091) -> None:
        if self._web_ui is None:
            self._web_ui = BackseatWebUI(self, host=host, port=port)
        self._web_ui.start()

    def stop_web_ui(self) -> None:
        if self._web_ui is not None:
            self._web_ui.stop()

    @property
    def is_network_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # -------------------------------------------------------------------------
    # Mission/FSM control
    # -------------------------------------------------------------------------
    def register_state(self, name: str, handler: StateHandler, *, initial: bool = False) -> None:
        if not name:
            raise ValueError("State name cannot be empty.")
        self._state_handlers[name] = handler
        if initial or self._state_name is None:
            self._state_name = name

    @property
    def state_name(self) -> Optional[str]:
        with self._lock:
            return self._state_name

    def start_mission(self) -> None:
        self._command_queue.put(("start_mission", None))

    def pause_mission(self) -> None:
        self._command_queue.put(("pause_mission", None))

    def resume_mission(self) -> None:
        self._command_queue.put(("resume_mission", None))

    def stop_mission(self) -> None:
        self._command_queue.put(("stop_mission", None))

    def goto_state(self, name: str) -> None:
        self._command_queue.put(("goto_state", name))

    def latest(self, message_name: str) -> Any:
        with self._lock:
            return self._latest.get(message_name)

    def latest_timestamp(self, message_name: str) -> Optional[float]:
        with self._lock:
            return self._latest_ts.get(message_name)

    def peers(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._peers)

    def resolve_target_src(self) -> Optional[int]:
        if self.target_system is None:
            return None
        with self._lock:
            entry = self._peers.get(self.target_system)
            if entry is None:
                return None
            return entry.get("src")

    # -------------------------------------------------------------------------
    # Outbound communication
    # -------------------------------------------------------------------------
    def register_comm_handler(self, channel: str, handler: CommHandler) -> None:
        self._comm_handlers[channel] = handler

    def queue_communication(self, channel: str, payload: Any, **options: Any) -> None:
        self._outbound_queue.put(_OutboundItem(channel=channel, payload=payload, options=dict(options)))

    def queue_imc(self, message: Any, **send_kwargs: Any) -> None:
        self.queue_communication("imc", message, **send_kwargs)

    def queue_sms(self, text: str, *, ttl: int = 60, destination: str = "broadcast") -> None:
        self.queue_communication("sms", text, ttl=ttl, destination=destination)

    def queue_iridium(self, payload: Any, *, ttl: int = 60, destination: str = "broadcast") -> None:
        self.queue_communication("iridium", payload, ttl=ttl, destination=destination)

    # -------------------------------------------------------------------------
    # Follow reference helpers
    # -------------------------------------------------------------------------
    def request_follow_reference(
        self,
        *,
        plan_id: str = "py_backseat",
        timeout: int = 10,
        loiter_radius: float = 0.0,
        altitude_interval: float = 0.0,
        control_src: Optional[int] = None,
        control_ent: int = 0xFF,
        dst: Optional[int] = None,
        ignore_errors: bool = True,
    ) -> bool:
        pc_cls = getattr(_pg.messages, "PlanControl")
        fr_cls = getattr(_pg.messages, "FollowReference")

        fr = fr_cls()
        if control_src is None:
            control_src = 0xFFFF
        fr.control_src = control_src
        fr.control_ent = control_ent
        fr.timeout = float(timeout)
        fr.loiter_radius = float(loiter_radius)
        fr.altitude_interval = float(altitude_interval)

        pc = pc_cls()
        pc.type = _pg.messages.PlanControl.TYPE.REQUEST
        pc.op = _pg.messages.PlanControl.OP.START
        pc.request_id = self._next_request_id()
        pc.plan_id = plan_id
        pc.arg = fr
        pc.info = "pyimclsts backseat follow-reference request"
        pc.flags = _pg.messages.PlanControl.FLAGS.IGNORE_ERRORS

        self.queue_imc(pc, dst=dst)
        return True

    def stop_plan(self, *, plan_id: Optional[str] = None, dst: Optional[int] = None) -> bool:
        pc_cls = getattr(_pg.messages, "PlanControl")

        pc = pc_cls()
        pc.type = pc_cls.TYPE.REQUEST
        pc.op = pc_cls.OP.STOP
        pc.request_id = self._next_request_id()
        pc.plan_id = "" if plan_id is None else plan_id
        pc.flags = pc_cls.FLAGS.EMPTY
        pc.arg = None
        pc.info = ""

        self.queue_imc(pc, dst=dst)
        return True

    def send_reference(
        self,
        *,
        lat: float,
        lon: float,
        lat_lon_radians: bool = False,
        depth: Optional[float] = None,
        altitude: Optional[float] = None,
        speed: Optional[float] = None,
        speed_units: str = "METERS_PS",
        radius: Optional[float] = None,
        dst: Optional[int] = None,
        dst_ent: Optional[int] = None,
    ) -> bool:
        try:
            ref_cls = getattr(_pg.messages, "Reference")
            dz_cls = getattr(_pg.messages, "DesiredZ")
            ds_cls = getattr(_pg.messages, "DesiredSpeed")
        except Exception:
            self.log("Reference/DesiredSpeed/DesiredZ classes not available in pyimc_generated.")
            return False

        ref = ref_cls()
        flags = ref_cls.FLAGS.LOCATION
        if speed is not None:
            flags = flags | ref_cls.FLAGS.SPEED
        if depth is not None or altitude is not None:
            flags = flags | ref_cls.FLAGS.Z
        if radius is not None:
            flags = flags | ref_cls.FLAGS.RADIUS

        ref.lat = lat if lat_lon_radians else math.radians(lat)
        ref.lon = lon if lat_lon_radians else math.radians(lon)
        if radius is None:
            ref.radius = 0.0
        else:
            ref.radius = float(radius)
        ref.flags = flags

        if speed is not None:
            spd = ds_cls()
            spd.value = float(speed)
            try:
                spd.speed_units = _pg.enumerations.SpeedUnits[speed_units.upper()]
            except Exception:
                spd.speed_units = _pg.enumerations.SpeedUnits.METERS_PS
            ref.speed = spd

        if depth is not None or altitude is not None:
            z = dz_cls()
            if depth is not None:
                z.value = float(depth)
                z.z_units = _pg.enumerations.ZUnits.DEPTH
            else:
                z.value = float(altitude)
                z.z_units = _pg.enumerations.ZUnits.ALTITUDE
            ref.z = z

        self.queue_imc(ref, dst=dst, dst_ent=dst_ent)
        return True

    # -------------------------------------------------------------------------
    # Status and logs
    # -------------------------------------------------------------------------
    def logs(self) -> List[str]:
        with self._lock:
            return list(self._logs)

    def log(self, text: str) -> None:
        line = "%s | %s" % (_utc_now_iso(), text)
        with self._lock:
            self._logs.append(line)
        print(line)

    def status_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            latest = {name: self._latest_ts.get(name) for name in self._latest.keys()}
            peer_list = []
            for name, info in self._peers.items():
                peer_list.append({"name": name, "src": info.get("src"), "last_seen": info.get("last_seen")})

            return {
                "network_running": self.is_network_running,
                "mission_active": self._mission_active,
                "paused": self._paused,
                "finished": self._finished,
                "state": self._state_name,
                "target_system": self.target_system,
                "target_src": self.resolve_target_src(),
                "known_peers": sorted(peer_list, key=lambda x: x["name"]),
                "latest_messages": latest,
                "queued_outbound": self._outbound_queue.qsize(),
                "queued_commands": self._command_queue.qsize(),
            }

    # -------------------------------------------------------------------------
    # Internal: network and subscriptions
    # -------------------------------------------------------------------------
    def _run_subscriber(self) -> None:
        self.log("Starting IMC subscriber on %s:%s" % (self.host, self.port))
        try:
            self._subscriber.run()
        except Exception as exc:  # pragma: no cover - runtime path
            self.log("Subscriber crashed: %s" % exc)
            self.log(traceback.format_exc())
        finally:
            self.log("IMC subscriber stopped")

    def _register_builtin_subscriptions(self) -> None:
        # Peer/system discovery
        self._subscribe_if_available("Announce", self._on_announce)

        # Safety
        self._subscribe_if_available("Abort", self._on_abort)

        # Common state messages cached for mission logic.
        for name in (
            "EstimatedState",
            "FollowRefState",
            "PlanControlState",
            "VehicleMedium",
            "GpsFix",
            "FuelLevel",
            "TransmissionStatus",
            "IridiumTxStatus",
            "TextMessage",
            "EntityInfo",
            "EntityList",
        ):
            self._subscribe_if_available(name, self._on_cache_message)

    def _subscribe_if_available(self, msg_name: str, callback: Callable[..., Any]) -> None:
        msg_cls = getattr(_pg.messages, msg_name, None)
        if msg_cls is None:
            return
        self._subscriber.subscribe_async(callback, msg_cls)

    def _on_cache_message(self, msg: Any, _send: Callable[..., None]) -> None:
        name = type(msg).__name__
        now = time.time()
        with self._lock:
            self._latest[name] = msg
            self._latest_ts[name] = now

    def _on_announce(self, msg: Any, _send: Callable[..., None]) -> None:
        self._on_cache_message(msg, _send)
        header = getattr(msg, "_header", None)
        src = getattr(header, "src", None)
        sys_name = msg.sys_name
        services_raw = msg.services
        services_list = []
        if services_raw:
            services_list = [s.strip() for s in services_raw.split(";") if s.strip()]
        if not sys_name:
            return
        with self._lock:
            entry = self._peers.get(sys_name, {})
            entry["src"] = src
            entry["last_seen"] = time.time()
            entry["services"] = services_raw
            entry["services_list"] = services_list
            self._peers[sys_name] = entry

    def _on_abort(self, msg: Any, _send: Callable[..., None]) -> None:
        self._on_cache_message(msg, _send)
        self.log("Received ABORT. Pausing mission.")
        self.pause_mission()

    # -------------------------------------------------------------------------
    # Internal: periodic callbacks
    # -------------------------------------------------------------------------
    def _control_tick(self, send: Callable[..., None]) -> None:
        self._drain_commands()

        with self._lock:
            mission_active = self._mission_active
            paused = self._paused
            finished = self._finished
            state_name = self._state_name

        if not mission_active or paused or finished:
            return

        if state_name is None:
            self.log("No state selected. Pausing mission.")
            self.pause_mission()
            return

        handler = self._state_handlers.get(state_name)
        if handler is None:
            self.log("Unknown state '%s'. Pausing mission." % state_name)
            self.pause_mission()
            return

        ctx = BackseatContext(self, send)
        try:
            next_state = handler(ctx)
        except Exception as exc:  # pragma: no cover - mission-specific runtime path
            self.log("Exception in state '%s': %s" % (state_name, exc))
            self.log(traceback.format_exc())
            self.pause_mission()
            return

        if next_state and next_state != state_name:
            if next_state not in self._state_handlers:
                self.log("State '%s' returned unknown next state '%s'." % (state_name, next_state))
                self.pause_mission()
                return
            with self._lock:
                self._state_name = next_state
            self.on_state_transition(state_name, next_state)

    def _communication_tick(self, send: Callable[..., None]) -> None:
        handled = 0
        while handled < 128:
            try:
                item = self._outbound_queue.get_nowait()
            except queue.Empty:
                break

            handled += 1
            if not isinstance(item, _OutboundItem):
                continue

            handler = self._comm_handlers.get(item.channel)
            if handler is None:
                self.log("No handler registered for communication channel '%s'." % item.channel)
                continue
            try:
                handler(send, item.payload, item.options)
            except Exception as exc:  # pragma: no cover - runtime path
                self.log("Error on comm channel '%s': %s" % (item.channel, exc))
                self.log(traceback.format_exc())

    def _drain_commands(self) -> None:
        while True:
            try:
                command, arg = self._command_queue.get_nowait()
            except queue.Empty:
                break

            if command == "start_mission":
                with self._lock:
                    if self._state_name is None and self._state_handlers:
                        self._state_name = next(iter(self._state_handlers.keys()))
                    self._mission_active = True
                    self._paused = False
                    self._finished = False
                self.on_start_mission()
                self.log("Mission started")

            elif command == "pause_mission":
                with self._lock:
                    self._paused = True
                self.on_pause_mission()
                self.log("Mission paused")

            elif command == "resume_mission":
                with self._lock:
                    if not self._finished:
                        self._mission_active = True
                        self._paused = False
                self.on_resume_mission()
                self.log("Mission resumed")

            elif command == "stop_mission":
                with self._lock:
                    self._paused = True
                    self._mission_active = False
                    self._finished = True
                self.on_stop_mission()
                self.log("Mission stopped")

            elif command == "goto_state":
                if arg not in self._state_handlers:
                    self.log("Cannot switch to unknown state '%s'" % arg)
                    continue
                with self._lock:
                    previous = self._state_name
                    self._state_name = arg
                if previous != arg:
                    self.on_state_transition(previous, arg)
                self.log("State forced to '%s'" % arg)

    # -------------------------------------------------------------------------
    # Internal: communication handlers
    # -------------------------------------------------------------------------
    def _send_now(self, send: Callable[..., None], message: Any, **send_kwargs: Any) -> None:
        kwargs = dict(send_kwargs)
        if kwargs.get("dst") is None:
            target_src = self.resolve_target_src()
            if target_src is not None:
                kwargs["dst"] = target_src

        clean_kwargs = {}
        for key in ("src", "src_ent", "dst", "dst_ent"):
            if kwargs.get(key) is not None:
                clean_kwargs[key] = kwargs[key]
        try:
            send(message, **clean_kwargs)
        except Exception:
            msg_name = type(message).__name__
            self.log("Failed to send IMC message '%s'." % msg_name)
            raise

    def _handle_imc(self, send: Callable[..., None], payload: Any, options: Dict[str, Any]) -> None:
        self._send_now(send, payload, **options)

    def _handle_sms(self, send: Callable[..., None], payload: Any, options: Dict[str, Any]) -> None:
        ttl = int(options.get("ttl", 60))
        destination = str(options.get("destination", "broadcast"))
        request = self._build_transmission_request(payload, ttl=ttl, destination=destination, mean="gsm")
        if request is not None:
            self._send_now(send, request)
            return

        # Fallback when TransmissionRequest is unavailable.
        txt_cls = getattr(_pg.messages, "TextMessage", None)
        if txt_cls is None:
            self.log("TextMessage class unavailable. Dropping SMS payload.")
            return
        txt = txt_cls()
        txt.origin = ""
        txt.text = str(payload)
        self._send_now(send, txt)

    def _handle_iridium(self, send: Callable[..., None], payload: Any, options: Dict[str, Any]) -> None:
        ttl = int(options.get("ttl", 60))
        destination = str(options.get("destination", "broadcast"))
        request = self._build_transmission_request(payload, ttl=ttl, destination=destination, mean="satellite")
        if request is None:
            self.log("TransmissionRequest class unavailable. Dropping Iridium payload.")
            return
        self._send_now(send, request)

    def _build_transmission_request(
        self,
        payload: Any,
        *,
        ttl: int,
        destination: str,
        mean: str,
    ) -> Optional[Any]:
        tr_cls = getattr(_pg.messages, "TransmissionRequest", None)
        if tr_cls is None:
            return None

        req = tr_cls()
        req.req_id = self._next_request_id()
        req.destination = destination
        req.deadline = time.time() + max(ttl, 1)
        req.range = 0.0
        req.msg_data = None
        req.txt_data = ""
        req.raw_data = b""

        mode = mean.lower()
        if mode == "satellite":
            req.comm_mean = tr_cls.COMM_MEAN.SATELLITE
        elif mode == "gsm":
            req.comm_mean = tr_cls.COMM_MEAN.GSM
        else:
            req.comm_mean = tr_cls.COMM_MEAN.ACOUSTIC

        if isinstance(payload, str):
            req.data_mode = tr_cls.DATA_MODE.TEXT
            req.txt_data = payload
        else:
            req.data_mode = tr_cls.DATA_MODE.INLINEMSG
            req.msg_data = payload

        return req

    # -------------------------------------------------------------------------
    # Internal: ids and hooks
    # -------------------------------------------------------------------------
    def _next_request_id(self) -> int:
        with self._lock:
            self._request_id = (self._request_id + 1) & 0xFFFF
            return self._request_id

    def on_start_mission(self) -> None:
        """Hook called when mission transitions to started."""

    def on_pause_mission(self) -> None:
        """Hook called when mission transitions to paused."""

    def on_resume_mission(self) -> None:
        """Hook called when mission transitions to resumed."""

    def on_stop_mission(self) -> None:
        """Hook called when mission transitions to stopped."""

    def on_state_transition(self, old: Optional[str], new: Optional[str]) -> None:
        self.log("FSM transition: %s -> %s" % (old, new))

    # -------------------------------------------------------------------------
    # Internal: simple web index page
    # -------------------------------------------------------------------------
    def _render_web_index(self) -> str:
        return """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>PyIMCLSTS Backseat</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #f4f7fb;
      --panel: #ffffff;
      --ink: #1e2a37;
      --muted: #5d6a79;
      --accent: #0d6e9f;
      --border: #d7e0e7;
    }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at 10% -10%, #dceef9 0%, var(--bg) 50%);
      color: var(--ink);
    }
    .wrap {
      max-width: 1100px;
      margin: 1.5rem auto;
      padding: 0 1rem 2rem;
      display: grid;
      gap: 1rem;
      grid-template-columns: 1fr;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 1rem;
      box-shadow: 0 4px 18px rgba(8, 36, 58, 0.06);
    }
    h1 {
      margin: 0 0 0.5rem 0;
      font-size: 1.5rem;
    }
    h2 {
      margin: 0 0 0.5rem 0;
      font-size: 1.1rem;
    }
    .muted {
      color: var(--muted);
    }
    .row {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      align-items: center;
    }
    button {
      border: 1px solid var(--border);
      background: #fff;
      color: var(--ink);
      border-radius: 8px;
      padding: 0.45rem 0.7rem;
      cursor: pointer;
      transition: transform .08s ease, border-color .2s ease;
    }
    button.primary {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }
    button:active {
      transform: translateY(1px);
    }
    textarea, input, select {
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.45rem 0.55rem;
      font-family: inherit;
    }
    textarea {
      width: 100%;
      min-height: 240px;
      font-family: "Iosevka", "Consolas", monospace;
      font-size: 0.88rem;
    }
    pre {
      margin: 0;
      overflow: auto;
      font-family: "Iosevka", "Consolas", monospace;
      font-size: 0.84rem;
    }
    .grid {
      display: grid;
      gap: 1rem;
      grid-template-columns: 1fr;
    }
    @media (min-width: 980px) {
      .grid {
        grid-template-columns: 1fr 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>PyIMCLSTS Generic Backseat</h1>
      <p class="muted">Control and inspect one backseat process from this page.</p>
      <div class="row">
        <button onclick="post('/api/network/start')" class="primary">Network Start</button>
        <button onclick="post('/api/network/stop')">Network Stop</button>
        <button onclick="post('/api/mission/start')" class="primary">Mission Start</button>
        <button onclick="post('/api/mission/pause')">Pause</button>
        <button onclick="post('/api/mission/resume')">Resume</button>
        <button onclick="post('/api/mission/stop')">Mission Stop</button>
      </div>
    </section>

    <section class="panel grid">
      <div>
        <h2>Status</h2>
        <pre id="status">Loading...</pre>
      </div>
      <div>
        <h2>Communication</h2>
        <div class="row">
          <select id="channel">
            <option value="sms">sms</option>
            <option value="iridium">iridium</option>
          </select>
          <input id="ttl" type="number" min="1" value="60" />
          <input id="destination" value="broadcast" />
        </div>
        <div class="row" style="margin-top: .5rem;">
          <input id="payload" style="flex:1;" placeholder="message payload"/>
          <button onclick="sendComm()">Send</button>
        </div>
      </div>
    </section>

    <section class="panel">
      <h2>Logs</h2>
      <textarea id="logs" readonly></textarea>
    </section>
  </div>

  <script>
    async function post(path, body = {}) {
      await fetch(path, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(body)
      });
      await refresh();
    }

    async function sendComm() {
      const body = {
        channel: document.getElementById("channel").value,
        ttl: Number(document.getElementById("ttl").value || 60),
        destination: document.getElementById("destination").value || "broadcast",
        payload: document.getElementById("payload").value || ""
      };
      await post("/api/comm/send", body);
      document.getElementById("payload").value = "";
    }

    async function refresh() {
      const s = await fetch("/api/status").then(r => r.json());
      const l = await fetch("/api/logs").then(r => r.json());
      document.getElementById("status").textContent = JSON.stringify(s, null, 2);
      document.getElementById("logs").value = (l.logs || []).join("\\n");
      const ta = document.getElementById("logs");
      ta.scrollTop = ta.scrollHeight;
    }

    refresh();
    setInterval(refresh, 1500);
  </script>
</body>
</html>
"""
