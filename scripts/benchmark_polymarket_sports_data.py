#!/usr/bin/env python3
"""Local recorder normalization/write benchmark against twice a measured peak."""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

ensure_src_on_path()


from oddsfox_pipeline.ingestion.polymarket.sports_data.books import (  # noqa: E402
    normalize_message,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (  # noqa: E402
    RAW_SCHEMA,
    UPDATE_SCHEMA,
    Limits,
    Segments,
    Store,
    sha256,
)


def benchmark(store: Store, repetitions=4):
    with store.sql_connection() as conn:
        return _benchmark(conn, store, repetitions)


def _benchmark(conn, store, repetitions):
    files, inputs = [], []
    for path, manifest in store.manifests("record"):
        files.extend(manifest["raw"])
        inputs.append(
            {"path": str(path.relative_to(store.root)), "sha256": sha256(path)}
        )
    if not files:
        raise ValueError(
            "measured recording input required; a synthetic rate is not a measured peak"
        )
    conn.read_parquet([str(store.validate_file(item)) for item in files]).create_view(
        "raw"
    )
    peaks = conn.execute("""WITH rate AS (
        SELECT date_trunc('second',received_at) second,count(*) messages,
               sum(octet_length(payload)) bytes FROM raw WHERE kind='message' GROUP BY 1)
        SELECT * FROM rate QUALIFY row_number() OVER(ORDER BY messages DESC,second)=1
           OR row_number() OVER(ORDER BY bytes DESC,second)=1 ORDER BY second""").fetchall()
    if not peaks:
        raise ValueError("no delivered market-channel messages to benchmark")
    identity = uuid4().hex
    writer = Segments(store, f"benchmarks/{identity}/updates", UPDATE_SCHEMA)
    raw_writer = Segments(store, f"benchmarks/{identity}/raw", RAW_SCHEMA)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    count, errors, cases = 0, 0, []
    for peak in peaks:
        # Isolate one measured second before timing; repeatedly scanning the
        # complete recording would measure history selection, not recorder rate.
        conn.execute(
            """CREATE OR REPLACE TEMP TABLE peak_raw AS SELECT * FROM raw
            WHERE kind='message' AND received_at >= ? AND received_at < ?""",
            [peak[0], peak[0] + timedelta(seconds=1)],
        )
        started, case_count = time.perf_counter(), 0
        for repetition in range(repetitions):
            for batch in conn.execute(
                "SELECT * FROM peak_raw ORDER BY connection,sequence",
            ).to_arrow_reader(batch_size=256):
                for row in batch.to_pylist():
                    count += 1
                    case_count += 1
                    stream = f"{peak[0]}/{repetition}/{row['connection']}"
                    raw_writer.append(
                        row
                        | {
                            "source": "benchmark",
                            "session": identity,
                            "connection": stream,
                        }
                    )
                    try:
                        normalized = normalize_message(
                            row["payload"],
                            source="benchmark",
                            stream=stream,
                            sequence=row["sequence"],
                            received_at=row["received_at"],
                            identity=f"{stream}:{row['sequence']}",
                        )
                        for item in normalized:
                            writer.append(item)
                    except (ValueError, KeyError, TypeError):
                        errors += 1
        writer.flush()
        raw_writer.flush()
        elapsed = time.perf_counter() - started
        cases.append(
            {
                "peak_second": peak[0],
                "observed_messages": peak[1],
                "observed_bytes": peak[2],
                "elapsed_seconds": elapsed,
                "achieved_messages_per_second": case_count / elapsed,
                "passed_rate": case_count / elapsed >= 2 * peak[1],
            }
        )
    output_files = writer.close() + raw_writer.close()
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux reports KiB.
    factor = 1 if sys.platform == "darwin" else 1024
    result = {
        "inputs": inputs,
        "cases": cases,
        "measured_peak_messages_per_second": max(peak[1] for peak in peaks),
        "processed_messages": count,
        "additional_peak_rss_bytes": max(0, after - before) * factor,
        "peak_rss_bytes": after * factor,
        "parse_errors": errors,
        "outputs": output_files,
        "scope": "message-rate and byte-rate peaks through bounded sample reads, normalization and raw/normalized Parquet writes; history selection excluded from timing; transport continuity tested separately",
    }
    result["passed"] = (
        all(case["passed_rate"] for case in cases)
        and errors == 0
        and max(0, after - before) * factor <= 512 * 1024**2
    )
    store.commit("benchmark", identity, output_files, inputs=inputs, result=result)
    store.atomic_json(f"benchmarks/{identity}/result.json", result)
    conn.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    store = Store(args.data_root, Limits())
    with store.writer():
        result = benchmark(store)
    print(json.dumps(result, default=str, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
