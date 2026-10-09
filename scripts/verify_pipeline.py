"""Run every local test suite and check state-machine graph integrity."""
import ast
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def check_graph(machine):
    states = machine["States"]
    assert machine["StartAt"] in states, "Unknown start state"
    for name, state in states.items():
        destinations = [state["Next"]] if "Next" in state else []
        destinations += [rule["Next"] for rule in state.get("Catch", [])]
        destinations += [rule["Next"] for rule in state.get("Choices", [])]
        if "Default" in state:
            destinations.append(state["Default"])
        assert all(target in states for target in destinations), name
        assert destinations or state.get("End") or state["Type"] in ("Fail", "Succeed"), name
        for branch in state.get("Branches", []):
            check_graph(branch)


def main():
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode().split("\0")
    for filename in tracked:
        path = ROOT / filename
        if path.suffix == ".py":
            ast.parse(path.read_text(encoding="utf-8"), filename=filename)
        elif path.suffix == ".json":
            json.loads(path.read_text(encoding="utf-8"))
    machine = json.loads((ROOT / "step_function/pipeline_orchestration.json").read_text())
    check_graph(machine)
    payload = machine["States"]["IngestFromYouTubeAPI"]["Parameters"]["Payload"]
    assert payload["time.$"] == "$$.Execution.StartTime", "Retries need a stable snapshot time"
    failures = []
    for filename in tracked:
        if Path(filename).name.startswith("test_") and filename.endswith(".py"):
            print(f"Running {filename}", flush=True)
            if subprocess.run([sys.executable, str(ROOT / filename)], cwd=ROOT).returncode:
                failures.append(filename)
    if failures:
        print(f"Failed suites: {failures}")
        return 1
    print("Local checks passed. AWS deployment and execution are not verified by these checks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
