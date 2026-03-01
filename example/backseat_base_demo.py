"""Example: build a simple follow-reference backseat with the generic base."""

import argparse
import time

import pyimc_generated as pg
from pyimclsts.backseat import BaseBackseat, BackseatContext


class DemoFollowRefBackseat(BaseBackseat):
    def __init__(self, host: str, port: int, target_system: str) -> None:
        super().__init__(host=host, port=port, target_system=target_system)
        self.vehicle_state = None
        self.follow_ref_request_time = 0.0
        self.follow_ref_retry_secs = 5.0

        self.register_state("wait_vehicle_service", self.wait_vehicle_service, initial=True)
        self.register_state("wait_estimated_state", self.wait_estimated_state)
        self.register_state("request_follow_reference", self.request_follow_reference_state)
        self.register_state("wait_maneuver_mode", self.wait_maneuver_mode)
        self.register_state("guide", self.guide)
        self._subscriber.subscribe_async(self.on_vehicle_state, pg.messages.VehicleState)

    def on_start_mission(self) -> None:
        # Mission flow starts only after vehicle enters SERVICE mode.
        self.goto_state("wait_vehicle_service")

    def on_resume_mission(self) -> None:
        # On resume, re-check vehicle mode before requesting FollowReference.
        self.goto_state("wait_vehicle_service")

    def on_vehicle_state(self, msg: pg.messages.VehicleState, _send_callback) -> None:
        self.vehicle_state = msg

    def wait_vehicle_service(self, _ctx: BackseatContext) -> str:
        if self.vehicle_state is None:
            return "wait_vehicle_service"
        if self.vehicle_state.op_mode != pg.messages.VehicleState.OP_MODE.SERVICE:
            return "wait_vehicle_service"
        return "wait_estimated_state"

    def wait_estimated_state(self, ctx: BackseatContext) -> str:
        if ctx.estimated_state is None:
            return "wait_estimated_state"
        return "request_follow_reference"

    def request_follow_reference_state(self, _ctx: BackseatContext) -> str:
        self.request_follow_reference(plan_id="py_backseat")
        self.follow_ref_request_time = time.time()
        return "wait_maneuver_mode"

    def wait_maneuver_mode(self, _ctx: BackseatContext) -> str:
        # Only start sending references once vehicle entered MANEUVER mode.
        if self.vehicle_state is not None and self.vehicle_state.op_mode == pg.messages.VehicleState.OP_MODE.MANEUVER:
            return "guide"

        # If vehicle did not transition, request FollowReference again.
        if time.time() - self.follow_ref_request_time >= self.follow_ref_retry_secs:
            return "request_follow_reference"
        return "wait_maneuver_mode"

    def guide(self, ctx: BackseatContext) -> str:
        est = ctx.estimated_state
        if est is None:
            return "guide"

        # EstimatedState latitude/longitude are in radians.
        self.send_reference(
            lat=est.lat + 0.00015,
            lon=est.lon,
            lat_lon_radians=True,
            depth=1.0,
            speed=50.0,
            speed_units="PERCENTAGE",
        )
        return "guide"


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo backseat using pyimclsts.backseat.BaseBackseat")
    parser.add_argument("--host", default="127.0.0.1", help="DUNE host")
    parser.add_argument("--port", type=int, default=6006, help="DUNE IMC TCP port")
    parser.add_argument("--target", default="lauv-xplore-2", help="Vehicle system name")
    parser.add_argument("--web-port", type=int, default=8091, help="Web UI port")
    parser.add_argument("--no-web", action="store_true", help="Disable built-in web UI")
    return parser.parse_args()


def main() -> None:
    args = _args()
    backseat = DemoFollowRefBackseat(args.host, args.port, args.target)
    backseat.start_network()

    if not args.no_web:
        backseat.start_web_ui(port=args.web_port)

    backseat.start_mission()

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        backseat.stop_mission()
        backseat.stop_web_ui()
        backseat.stop_network()


if __name__ == "__main__":
    main()
