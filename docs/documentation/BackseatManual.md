# Generic Backseat Manual (`pyimclsts/src/pyimclsts/backseat.py`)

This manual documents the generic backseat framework implemented in:

- `pyimclsts/src/pyimclsts/backseat.py`

The module provides a reusable control layer for IMC vehicles built on top of `pyimclsts.network.subscriber`.

## 1. Purpose and Scope

`backseat.py` gives you a base class to build mission-specific autonomy processes with four built-in concerns:

1. IMC TCP connection lifecycle.
2. Generic finite-state-machine (FSM) mission loop.
3. Communication queue and channel handlers (`imc`, `sms`, `iridium`).
4. Lightweight HTTP control UI (start/stop/pause/status/logs).

It is intended to be subclassed (you implement mission behavior via states/hooks).

## 2. Main Types

The module exposes three public classes:

1. `BackseatContext`
2. `BackseatWebUI`
3. `BaseBackseat`

`BaseBackseat` is the main framework class; the others are support classes.

---

## 3. `BackseatContext`

`BackseatContext` is the object passed into each FSM state handler.

### Properties

- `backseat`: current `BaseBackseat` instance.
- `state_name`: current FSM state name.
- `estimated_state`: latest cached `EstimatedState` message (or `None`).
- `follow_ref_state`: latest cached `FollowRefState` message (or `None`).
- `plan_control_state`: latest cached `PlanControlState` message (or `None`).

### Methods

- `send_now(message, **send_kwargs)`: immediate send through active IMC send callback.
- `queue(channel, payload, **options)`: enqueue outbound communication.
- `goto_state(state_name)`: utility that returns a state name (useful in handlers).
- `log(text)`: append module log line.

### Typical use in state handlers

```python
def my_state(ctx: BackseatContext) -> str:
    if ctx.estimated_state is None:
        return "my_state"
    ctx.log("EstimatedState received")
    return "next_state"
```

---

## 4. `BackseatWebUI`

`BackseatWebUI` wraps `ThreadingHTTPServer` and provides:

- A simple HTML dashboard (`GET /`)
- JSON API endpoints
- Background thread server lifecycle

### Constructor

```python
BackseatWebUI(backseat, host="0.0.0.0", port=8091)
```

### Lifecycle

- `start()`: starts server thread.
- `stop()`: shutdown + close + join.
- `is_running`: property.

### HTTP API

#### GET

- `/`: HTML dashboard
- `/api/status`: `BaseBackseat.status_snapshot()`
- `/api/logs`: log buffer

#### POST

- `/api/network/start`
- `/api/network/stop`
- `/api/mission/start`
- `/api/mission/pause`
- `/api/mission/resume`
- `/api/mission/stop`
- `/api/comm/send` with body:
  - `channel` (`sms` or `iridium` by default UI)
  - `payload`
  - `ttl`
  - `destination`

---

## 5. `BaseBackseat`: Architecture

`BaseBackseat` owns all runtime state:

- Network/subscriber.
- FSM registry and current state.
- Mission flags (`active`, `paused`, `finished`).
- Message cache (`_latest`, `_latest_ts`).
- Peer mapping (`_peers`).
- Command queue and outbound queue.
- Communication handler registry.
- Optional web UI object.

### Internal execution loops

There are two periodic loops registered in `__init__`:

1. `control_period` -> `_control_tick` (FSM and mission commands)
2. `comm_period` -> `_communication_tick` (outbound queue dispatch)

Both run inside the `pyimclsts.network.subscriber` event loop.

---

## 6. Constructor and Parameters

```python
BaseBackseat(
    host="127.0.0.1",
    port=6006,
    target_system=None,
    control_period=1.0,
    comm_period=1.0,
    big_endian=False,
    use_mp=False,
    log_limit=500,
)
```

### Key parameters

- `host`, `port`: IMC TCP endpoint.
- `target_system`: vehicle system name used to resolve destination `src`.
- `control_period`: seconds between FSM ticks.
- `comm_period`: seconds between outbound-queue flushes.
- `big_endian`, `use_mp`: forwarded to `subscriber`.
- `log_limit`: ring-buffer size.

### Prerequisite

If `pyimc_generated` cannot be imported, constructor raises `RuntimeError`.

---

## 7. Lifecycle API

### Network

- `start_network()`: starts subscriber in a daemon thread.
- `stop_network(timeout=3.0)`: calls `subscriber.stop()` and joins thread.
- `is_network_running`: thread alive check.

### Web UI

- `start_web_ui(host="0.0.0.0", port=8091)`
- `stop_web_ui()`

### Mission control (queued commands)

- `start_mission()`
- `pause_mission()`
- `resume_mission()`
- `stop_mission()`
- `goto_state(name)`

These are queued and applied in `_drain_commands` during `_control_tick`.

---

## 8. FSM Model

### Register states

Use:

```python
register_state(name: str, handler: Callable[[BackseatContext], Optional[str]], initial=False)
```

- `name`: state id.
- `handler`: receives `BackseatContext`, returns next state name or same/`None`.
- `initial=True`: sets default current state.

### State progression

Per `_control_tick`:

1. Command queue drained.
2. If mission inactive/paused/finished -> returns.
3. Current state handler is called.
4. If handler returns a different state name:
   - Validates name exists.
   - Updates current state.
   - Calls `on_state_transition(old, new)`.

### Safety behavior

- If state missing or invalid transition returned -> mission pauses.
- Exceptions in state handlers are caught, logged, and mission pauses.

---

## 9. Message Cache and Peer Tracking

`BaseBackseat` subscribes to a standard set of IMC messages and caches latest message by type name.

### Built-in subscriptions

- Discovery/safety:
  - `Announce`
  - `Abort`
- Cached state:
  - `EstimatedState`
  - `FollowRefState`
  - `PlanControlState`
  - `VehicleMedium`
  - `GpsFix`
  - `FuelLevel`
  - `TransmissionStatus`
  - `IridiumTxStatus`
  - `TextMessage`
  - `EntityInfo`
  - `EntityList`

### Access helpers

- `latest(message_name)`
- `latest_timestamp(message_name)`
- `peers()`
- `resolve_target_src()`

`resolve_target_src()` maps `target_system` -> peer `src` based on `Announce`.

---

## 10. Communication Queue and Handlers

Outbound communication is queued as `_OutboundItem(channel, payload, options)` and processed in `_communication_tick`.

### Queue methods

- `queue_communication(channel, payload, **options)`
- `queue_imc(message, **send_kwargs)`
- `queue_sms(text, ttl=60, destination="broadcast")`
- `queue_iridium(payload, ttl=60, destination="broadcast")`

### Handler registration

- `register_comm_handler(channel, handler)`

Default handlers:

- `"imc"` -> `_handle_imc`
- `"sms"` -> `_handle_sms`
- `"iridium"` -> `_handle_iridium`

### `_send_now` behavior

If `dst` is not provided in send kwargs and `target_system` resolves to a peer source, that source is used as destination.

---

## 11. Follow Reference Convenience Methods

The class includes helpers for common FollowReference workflows.

### `request_follow_reference(...)`

Builds and queues a `PlanControl(START)` containing a `FollowReference` maneuver.

Key defaults:

- `plan_id="py_backseat"`
- `timeout=10`
- `loiter_radius=0.0`
- `altitude_interval=0.0`
- `control_src=0xFFFF` when omitted
- `control_ent=0xFF`
- `flags=IGNORE_ERRORS` (current implementation always uses this flag)

### `stop_plan(...)`

Builds and queues a `PlanControl(STOP)`.

### `send_reference(...)`

Builds and queues a `Reference` message with optional speed and Z references.

Important behavior:

- `lat/lon` converted from degrees unless `lat_lon_radians=True`.
- `radius` defaults to `0.0` if omitted.
- `speed_units` resolves by enum name; invalid names fallback to `METERS_PS`.
- Z can be `DEPTH` or `ALTITUDE` based on provided args.

---

## 12. TransmissionRequest Builder

`_build_transmission_request(payload, ttl, destination, mean)` creates `TransmissionRequest` with:

- `req_id`: internal sequence.
- `deadline`: `time.time() + ttl`
- `comm_mean`: from `mean` (`satellite`, `gsm`, else `acoustic`)
- `data_mode`: `TEXT` if payload is `str`, else `INLINEMSG`
- Defaults:
  - `range = 0.0`
  - `msg_data = None`
  - `txt_data = ""`
  - `raw_data = b""`

Used by `_handle_sms` and `_handle_iridium`.

---

## 13. Hooks for Subclasses

Override these methods to customize behavior:

- `on_start_mission()`
- `on_pause_mission()`
- `on_resume_mission()`
- `on_stop_mission()`
- `on_state_transition(old, new)`

Typical use:

- Force state reset on mission start.
- Re-arm FollowReference on resume.
- Emit custom telemetry/logging.

---

## 14. Status and Logs

### `log(text)`

Adds timestamped line to ring buffer and prints to stdout.

### `logs()`

Returns buffered log lines.

### `status_snapshot()`

Returns dictionary with:

- network flags
- mission flags
- current state
- target system/source
- known peers
- latest message timestamps
- queue sizes

---

## 15. Minimal Subclass Example

```python
from pyimclsts.backseat import BaseBackseat, BackseatContext

class MyBackseat(BaseBackseat):
    def __init__(self, host, port, target):
        super().__init__(host=host, port=port, target_system=target)
        self.register_state("wait_est", self.wait_est, initial=True)
        self.register_state("request_fr", self.request_fr)
        self.register_state("guide", self.guide)

    def on_start_mission(self):
        self.goto_state("request_fr")

    def wait_est(self, ctx: BackseatContext):
        if ctx.estimated_state is None:
            return "wait_est"
        return "request_fr"

    def request_fr(self, ctx: BackseatContext):
        self.request_follow_reference(plan_id="my_plan")
        return "guide"

    def guide(self, ctx: BackseatContext):
        est = ctx.estimated_state
        if est is None:
            return "guide"
        self.send_reference(
            lat=est.lat,
            lon=est.lon,
            lat_lon_radians=True,
            depth=1.0,
            speed=50.0,
            speed_units="PERCENTAGE",
        )
        return "guide"

bs = MyBackseat("127.0.0.1", 6006, "lauv-xplore-2")
bs.start_network()
bs.start_web_ui(port=8091)
bs.start_mission()
```

---

## 16. Operational Notes and Caveats

1. `request_follow_reference()` currently sets `IGNORE_ERRORS` flag unconditionally in current implementation.
2. `_send_now()` does not auto-populate missing IMC scalar fields; if you queue custom messages, set all required non-message fields explicitly.
3. `BaseBackseat` assumes `pyimc_generated` is already extracted and importable in current working directory.
4. Web UI is intentionally simple and not authenticated; treat it as local-control tooling.

---

## 17. How To Construct a Backseat (Step-by-Step)

This section is a practical build recipe for a new project.

### Step 1: Define the subclass

Create your class and register states in `__init__`.

```python
from pyimclsts.backseat import BaseBackseat, BackseatContext

class SurveyBackseat(BaseBackseat):
    def __init__(self, host: str, port: int, target_system: str):
        super().__init__(host=host, port=port, target_system=target_system)

        self.register_state("wait_for_peer", self.wait_for_peer, initial=True)
        self.register_state("wait_for_nav", self.wait_for_nav)
        self.register_state("request_fr", self.request_fr)
        self.register_state("guide", self.guide)
```

### Step 2: Add state handlers

Each state receives `BackseatContext` and returns the next state name.

```python
def wait_for_peer(self, ctx: BackseatContext):
    if self.resolve_target_src() is None:
        return "wait_for_peer"
    return "wait_for_nav"

def wait_for_nav(self, ctx: BackseatContext):
    if ctx.estimated_state is None:
        return "wait_for_nav"
    return "request_fr"

def request_fr(self, ctx: BackseatContext):
    self.request_follow_reference(plan_id="survey")
    return "guide"

def guide(self, ctx: BackseatContext):
    est = ctx.estimated_state
    if est is None:
        return "guide"
    self.send_reference(
        lat=est.lat,
        lon=est.lon + 0.0002,
        lat_lon_radians=True,
        depth=1.5,
        speed=40.0,
        speed_units="PERCENTAGE",
    )
    return "guide"
```

### Step 3: Handle mission hooks

Use hooks to guarantee state reset on mission transitions.

```python
def on_start_mission(self):
    self.goto_state("request_fr")

def on_resume_mission(self):
    self.goto_state("request_fr")
```

### Step 4: Start runtime

```python
def main():
    bs = SurveyBackseat("127.0.0.1", 6006, "lauv-xplore-2")
    bs.start_network()
    bs.start_web_ui(port=8091)
    bs.start_mission()

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        bs.stop_mission()
        bs.stop_web_ui()
        bs.stop_network()
```

## 18. API Usage Patterns (Examples)

### Pattern A: Force re-request of FollowReference after mission restart

Use mission hooks instead of writing re-request logic inside every state:

```python
def on_start_mission(self):
    self.goto_state("request_fr")
```

### Pattern B: Wait for peer before sending to target

```python
def wait_for_peer(self, ctx: BackseatContext):
    if self.resolve_target_src() is None:
        ctx.log("Waiting for Announce from target...")
        return "wait_for_peer"
    return "next_state"
```

### Pattern C: Use explicit destination (multi-vehicle control)

```python
src = self.resolve_target_src()
if src is not None:
    self.send_reference(
        lat=41.18,
        lon=-8.70,
        depth=1.0,
        speed=1.2,
        speed_units="METERS_PS",
        dst=src,  # explicit destination
    )
```

### Pattern D: Pause mission on stale navigation

```python
def guide(self, ctx: BackseatContext):
    ts = self.latest_timestamp("EstimatedState")
    if ts is None or (time.time() - ts) > 10:
        self.log("EstimatedState timeout -> pause")
        self.pause_mission()
        return "guide"
    # normal guidance
    return "guide"
```

## 19. Building Messages Manually with `queue_imc`

You can send any IMC message directly via:

```python
self.queue_imc(message, dst=0x1234)
```

Important: set all required non-message fields before queueing.

### Example: `PlanControl` START

```python
pc = _pg.messages.PlanControl()
fr = _pg.messages.FollowReference()

fr.control_src = 0xFFFF
fr.control_ent = 0xFF
fr.timeout = 10.0
fr.loiter_radius = 0.0
fr.altitude_interval = 0.0

pc.type = _pg.messages.PlanControl.TYPE.REQUEST
pc.op = _pg.messages.PlanControl.OP.START
pc.request_id = 123
pc.plan_id = "my_plan"
pc.flags = _pg.messages.PlanControl.FLAGS.IGNORE_ERRORS
pc.arg = fr
pc.info = "start follow-ref"

self.queue_imc(pc)
```

### Example: `Reference`

```python
ref = _pg.messages.Reference()
ref.flags = (
    _pg.messages.Reference.FLAGS.LOCATION
    | _pg.messages.Reference.FLAGS.SPEED
    | _pg.messages.Reference.FLAGS.Z
)
ref.lat = math.radians(41.18)
ref.lon = math.radians(-8.70)
ref.radius = 0.0
ref.speed = _pg.messages.DesiredSpeed(
    value=50.0,
    speed_units=_pg.enumerations.SpeedUnits.PERCENTAGE,
)
ref.z = _pg.messages.DesiredZ(
    value=1.0,
    z_units=_pg.enumerations.ZUnits.DEPTH,
)
self.queue_imc(ref)
```

## 20. Custom Communication Channel Example

`BaseBackseat` lets you add custom outbound channels.

### Register custom channel

```python
def __init__(self, host, port, target):
    super().__init__(host=host, port=port, target_system=target)
    self.register_comm_handler("debug_log", self._handle_debug_log)
```

### Implement handler

```python
def _handle_debug_log(self, send, payload, options):
    self.log(f"[debug_log] {payload}")
```

### Queue from a state

```python
ctx.queue("debug_log", {"event": "entered_guide"})
```

## 21. Recommended State Layout for Real Missions

A robust layout is usually:

1. `wait_for_peer`
2. `wait_for_nav`
3. `request_follow_reference`
4. `transit`
5. `survey`
6. `surface_report`
7. `recover_or_pause`

This keeps each state small and makes mission logs easier to analyze.

## 22. Common Errors and Fixes

### `NoneType` serialization errors when sending IMC messages

Cause: one or more required scalar fields were not set.

Fix: initialize all required fields before calling `queue_imc` / `send_now`.

### Mission starts but vehicle does not react

Possible causes:

- `target_system` not seen yet (`Announce` missing).
- wrong IMC TCP host/port.
- you forgot to re-request FollowReference on mission start/resume.

### FSM seems frozen

Check:

- current `state` in `status_snapshot()`
- recent logs (`/api/logs`)
- `latest_messages` timestamps for stale data

## 23. Suggested Project Structure

```text
my_backseat/
  config/
    mission.ini
  my_backseat.py
  states.py
  comms.py
  main.py
```

Keep state logic separated from process bootstrap as the project grows.

## 24. File References

- Framework source:
  - `pyimclsts/src/pyimclsts/backseat.py`
- Demo subclass:
  - `pyimclsts/example/backseat_base_demo.py`
- Package export:
  - `pyimclsts/src/pyimclsts/__init__.py`
