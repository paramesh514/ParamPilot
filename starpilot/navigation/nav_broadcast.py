#!/usr/bin/env python3
"""ParamPilot companion-app telemetry broadcaster.

Broadcasts driving/navigation telemetry as JSON over UDP so a companion
Android app (ParamPilotAuto) can render a big-screen navigation display
(maneuver card, lane guidance) on the car's head unit via Android Auto.

Display-only: this process never accepts commands and never touches driving
logic. It reads the already-published NavInstructionState plus live car state
and re-broadcasts a compact JSON snapshot at 10 Hz on the local subnet.

Packet schema (v1), UDP port 28941, < 1472 bytes per datagram:
{
  "v": 1, "t": <unix time>,
  "speed_ms": 22.5, "cruise_active": true, "experimental_mode": false,
  "nav": {
    "valid": true,
    "maneuver_type": "turn", "maneuver_modifier": "left",
    "maneuver_distance_m": 150.0,
    "street": "E Bell Rd",
    "lane_count": 3,
    "active_lane_direction": "straight",
    "active_lane_index": 1,
    "same_side_lane_count": 2
  },
  "lead": {"present": true, "distance_m": 25.0}
}
"""
from __future__ import annotations

import json
import socket
import time

import cereal.messaging as messaging
from openpilot.common.params import Params
from openpilot.common.realtime import Ratekeeper
from openpilot.common.swaglog import cloudlog

NAV_BROADCAST_HZ = 10
NAV_BROADCAST_PORT = 28941
BROADCAST_ADDR = "255.255.255.255"
PROTOCOL_VERSION = 1


class NavBroadcast:
  def __init__(self) -> None:
    self.params = Params()
    self.params_memory = Params(memory=True)
    self.sm = messaging.SubMaster(["carState", "controlsState", "radarState"])
    self.rk = Ratekeeper(NAV_BROADCAST_HZ, print_delay_threshold=None)
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

  @staticmethod
  def _parse_nav_state(raw: object) -> dict:
    if not raw:
      return {}
    if isinstance(raw, dict):
      return raw
    if isinstance(raw, (bytes, bytearray)):
      raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str):
      try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
      except Exception:
        return {}
    return {}

  def _build_packet(self) -> dict:
    self.sm.update(0)
    car_state = self.sm["carState"]
    controls_state = self.sm["controlsState"]
    radar_state = self.sm["radarState"]

    nav_raw = self.params_memory.get("NavInstructionState")
    nav = self._parse_nav_state(nav_raw)

    nav_packet: dict = {"valid": False}
    if nav.get("valid"):
      nav_packet = {
        "valid": True,
        "maneuver_type": str(nav.get("maneuverType") or ""),
        "maneuver_modifier": str(nav.get("maneuverModifier") or ""),
        "maneuver_distance_m": float(nav.get("maneuverDistance") or 0.0),
        "street": str(nav.get("maneuverPrimaryText") or ""),
        "lane_count": int(nav.get("laneCount") or 0),
        "active_lane_direction": str(nav.get("activeLaneDirection") or ""),
        "active_lane_index": int(nav.get("activeLaneIndex") if nav.get("activeLaneIndex") is not None else -1),
        "same_side_lane_count": int(nav.get("sameSideLaneCount") or 0),
        "lanes": [
          {"directions": [str(d) for d in (lane.get("directions") or []) if d],
           "active": bool(lane.get("active", False))}
          for lane in (nav.get("lanes") or []) if isinstance(lane, dict)
        ],
      }

    lead_present = bool(radar_state.leadOne.status) if radar_state is not None else False
    lead_dist = float(radar_state.leadOne.dRel) if lead_present else 0.0

    return {
      "v": PROTOCOL_VERSION,
      "t": time.time(),
      "speed_ms": float(car_state.vEgo) if car_state is not None else 0.0,
      "cruise_active": bool(controls_state.active) if controls_state is not None else False,
      "experimental_mode": bool(self.params.get_bool("ExperimentalMode")),
      "nav": nav_packet,
      "lead": {"present": lead_present, "distance_m": lead_dist},
    }

  def run(self) -> None:
    cloudlog.warning("nav_broadcast init")
    while True:
      try:
        packet = self._build_packet()
        data = json.dumps(packet, separators=(",", ":")).encode("utf-8")
        if len(data) < 1472:
          self.sock.sendto(data, (BROADCAST_ADDR, NAV_BROADCAST_PORT))
        else:
          cloudlog.warning(f"nav_broadcast packet too large: {len(data)} bytes, dropped")
      except Exception as e:
        cloudlog.warning(f"nav_broadcast error: {e}")
      self.rk.keep_time()


def main() -> None:
  NavBroadcast().run()


if __name__ == "__main__":
  main()
