"""Verify ProtoMap's actual offline scan against the catalog, including fields and determinism."""

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from generate import ROOT, list_calls, read_catalog


def verify(source):
    source = source.resolve()
    if not (source / "crates/scanner/Cargo.toml").is_file():
        raise ValueError(f"Not a ProtoMap source repository: {source}")
    domains, services = read_catalog()
    def method_of(name, rpc):
        return f"fixture.{services[name]['domain']}.v1.{services[name]['name']}/{rpc}"

    methods = {name: method_of(name, service["rpc"]) for name, service in services.items()}
    # Every method a service implements, its main one and its operations, each with the peer methods it calls.
    handlers = {method_of(name, rpc): [method_of(peer, target) for peer, target in calls]
                for name, service in services.items() for rpc, calls in list_calls(services, service)}
    expected_edges = {(method, target): True for method, targets in handlers.items() for target in targets}
    with tempfile.TemporaryDirectory(prefix="protomap-fixture-verifier-") as folder:
        temp = Path(folder)
        (temp / "src").mkdir()
        manifest = '\n'.join([
            '[package]', 'name = "protomap-fixture-verifier"', 'version = "0.0.0"', 'edition = "2024"',
            '[dev-dependencies]',
            'protomap-scanner = { path = ' + json.dumps(str(source / "crates/scanner")) + ' }',
            'protomap-model = { path = ' + json.dumps(str(source / "crates/model")) + ' }', '',
        ])
        (temp / "Cargo.toml").write_text(manifest)
        shutil.copyfile(source / "Cargo.lock", temp / "Cargo.lock")
        report_path = temp / "report.tsv"
        code = (ROOT / "shell/scanner_check.rs").read_text()
        code = code.replace("FIXTURE_ROOT_LITERAL", json.dumps(str(ROOT)))
        code = code.replace("REPORT_PATH_LITERAL", json.dumps(str(report_path)))
        (temp / "src/lib.rs").write_text(code)
        subprocess.run(["cargo", "test", "--release", "--manifest-path", str(temp / "Cargo.toml"),
                        "--target-dir", str(source / "target"), "--", "--nocapture"], cwd=source, check=True)
        rows = [line.split("\t") for line in report_path.read_text().splitlines()]
    actual_services = [row[1] for row in rows if row[0] == "SERVICE"]
    edge_rows = [row for row in rows if row[0] == "EDGE"]
    actual_edges = {(row[1], row[2]): row[3] for row in edge_rows}
    if len(edge_rows) != len(actual_edges):
        raise ValueError("The scan contains duplicate peer call sites")
    if sorted(actual_services) != sorted(handlers):
        raise ValueError(f"Expected {len(handlers)} handlers, got {len(actual_services)}")
    if sorted(actual_edges) != sorted(expected_edges):
        missing = [edge for edge in expected_edges if edge not in actual_edges]
        extra = [edge for edge in actual_edges if edge not in expected_edges]
        raise ValueError(f"Peer edge mismatch: missing={missing[:5]}, extra={extra[:5]}")
    missing_flows = [edge for edge, found in actual_edges.items() if found != "true"]
    if missing_flows:
        raise ValueError(f"Missing certain request email flows: {missing_flows[:5]}")
    response_methods = {method: True for method, targets in handlers.items() if targets}
    missing_responses = [row[1] for row in rows if row[0] == "RESPONSE" and row[1] in response_methods
                         and row[2] != "true"]
    if missing_responses:
        raise ValueError(f"Missing certain downstream response flows: {missing_responses[:5]}")
    leaf_methods = {method: True for method, targets in handlers.items() if not targets}
    leaf_responses = {row[1]: row[2] for row in rows if row[0] == "LEAF_RESPONSE"}
    if any(leaf_responses.get(method) != "true" for method in leaf_methods):
        raise ValueError("Missing certain request-to-response flows for leaf services")
    stats = next(row for row in rows if row[0] == "STATS")
    baseline = {
        "services": len(services), "domains": len(domains), "handlers": len(actual_services),
        "peer_edges": len(actual_edges), "unresolved_service_calls": 0,
        "certain_request_email_flows": len(actual_edges), "response_flow_services": len(response_methods),
        "leaf_response_flow_services": len(leaf_methods),
        "scan_seconds": float(stats[1]), "artifact_bytes": int(stats[2]), "deterministic_across_threads": True,
    }
    auth = json.loads((ROOT / "catalog/services.json").read_text()).get("shared_auth_service")
    if auth is not None:
        callers = [source for source, target in actual_edges if target == methods[auth]]
        if sorted(callers) != sorted(method for name, method in methods.items() if name != auth):
            raise ValueError("The auth service must be called by every other service")
        baseline["auth_service"] = auth
        baseline["auth_callers"] = len(callers)
    (ROOT / ".cache").mkdir(exist_ok=True)
    (ROOT / ".cache/protomap-baseline.json").write_text(json.dumps(baseline, indent=2) + "\n")
    print(json.dumps(baseline, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path, default=ROOT.parent.parent / "protomap-source",
                        help="ProtoMap source checkout; defaults to the sibling protomap-source directory")
    args = parser.parse_args()
    try:
        verify(args.source)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"ProtoMap verification failed: {error}\n")


if __name__ == "__main__":
    main()
