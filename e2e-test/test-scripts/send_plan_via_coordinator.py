#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Push a full AgentPlan to a running Flink Agents job via the OperatorCoordinator
REST endpoint — from pure Python, no JVM.

The coordination request body wraps a Java-serialized ``PlanUpdateRequest`` (a
Serializable with a single ``String agentPlanJson`` field). That byte layout is
fixed, so we hand-build it: a constant class-descriptor prefix followed by a
length-prefixed UTF-8 string. No javaobj dependency.

The coordination endpoint's :operatorid path segment is the agent operator's
OperatorID. Flink builds differ in how they resolve it: some accept the operator
**uid** and hash it server-side, others require the **hex** OperatorID (which equals
the operator vertex id exposed by GET /jobs/{jobId}). This client tries the hex first
and falls back to the uid, so it works on either — and needs no JVM.

Usage:
    send_plan_via_coordinator.py <rest_base> <job_id> <plan_json_file>
    send_plan_via_coordinator.py --emit-bytes <plan_json_file> <out_file>
"""
import base64
import json
import struct
import sys
import urllib.error
import urllib.parse
import urllib.request

# Must match the uid / transform name set in CompileUtils.coordinatedConnect.
_OPERATOR_UID = "flink-agents-coordinated-operator"
_OPERATOR_NAME = "coordinated-action-execute-operator"


def _resolve_operator_id_hex(rest_base: str, job_id: str) -> str | None:
    """The agent operator's OperatorID equals its vertex id in GET /jobs/{jobId}."""
    try:
        with urllib.request.urlopen(f"{rest_base}/jobs/{job_id}", timeout=5) as resp:
            plan = json.load(resp)
    except OSError:
        return None
    for vertex in plan.get("vertices", []):
        if _OPERATOR_NAME in vertex.get("name", ""):
            return vertex.get("id")
    return None


def _post(rest_base: str, job_id: str, operator_id: str, body: bytes) -> int:
    url = f"{rest_base}/jobs/{job_id}/coordinators/{urllib.parse.quote(operator_id, safe='')}"
    req = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        print(f"POST {url} -> {resp.status} {resp.read().decode('utf-8', 'replace')}")
        return resp.status

_CLASS = (
    "org.apache.flink.agents.runtime.operator.coordinator."
    "PlanUpdateMessages$PlanUpdateRequest"
)
# java.io serialization tags
_MAGIC = b"\xac\xed\x00\x05"
_TC_OBJECT, _TC_CLASSDESC, _TC_ENDBLOCKDATA, _TC_NULL, _TC_STRING, _TC_LONGSTRING = (
    0x73,
    0x72,
    0x78,
    0x70,
    0x74,
    0x7C,
)


def _utf(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack(">H", len(b)) + b


def _java_string(s: str) -> bytes:
    b = s.encode("utf-8")
    if len(b) < 65536:
        return bytes([_TC_STRING]) + struct.pack(">H", len(b)) + b
    return bytes([_TC_LONGSTRING]) + struct.pack(">q", len(b)) + b


def serialize_plan_update_request(plan_json: str) -> bytes:
    """Reproduce ObjectOutputStream output for new PlanUpdateRequest(plan_json)."""
    out = bytearray(_MAGIC)
    out += bytes([_TC_OBJECT, _TC_CLASSDESC])
    out += _utf(_CLASS)
    out += struct.pack(">q", 1)  # serialVersionUID = 1L
    out += bytes([0x02])  # flags: SC_SERIALIZABLE
    out += struct.pack(">H", 1)  # field count
    out += bytes([ord("L")]) + _utf("agentPlanJson")  # object field
    out += _java_string("Ljava/lang/String;")  # field type signature
    out += bytes([_TC_ENDBLOCKDATA])  # no class annotations
    out += bytes([_TC_NULL])  # no superclass descriptor
    out += _java_string(plan_json)  # classdata: the single String field
    return bytes(out)


def main() -> int:
    # Test/inspection mode: just emit the serialized request bytes to a file.
    if sys.argv[1] == "--emit-bytes":
        plan_file, out_file = sys.argv[2:4]
        with open(plan_file, encoding="utf-8") as f:
            plan_json = f.read()
        with open(out_file, "wb") as f:
            f.write(serialize_plan_update_request(plan_json))
        return 0

    rest_base, job_id, plan_file = sys.argv[1:4]
    with open(plan_file, encoding="utf-8") as f:
        plan_json = f.read()

    payload = base64.b64encode(serialize_plan_update_request(plan_json)).decode("ascii")
    body = json.dumps({"serializedCoordinationRequest": payload}).encode("utf-8")

    # Try the hex OperatorID (GET /jobs-resolved) first, then the uid; Flink builds
    # differ in which one the :operatorid path segment accepts.
    candidates = [
        c
        for c in (_resolve_operator_id_hex(rest_base, job_id), _OPERATOR_UID)
        if c is not None
    ]
    last_error: Exception | None = None
    for operator_id in candidates:
        try:
            _post(rest_base, job_id, operator_id, body)
            return 0
        except urllib.error.HTTPError as e:  # surface the Flink error body, then retry
            detail = e.read().decode("utf-8", "replace")
            print(f"POST .../coordinators/{operator_id} -> {e.code} {detail}")
            last_error = e
    if last_error is not None:
        raise last_error
    raise SystemExit("could not resolve the coordinator operator id")


if __name__ == "__main__":
    sys.exit(main())
