"""Corrupt a disposable full Silver copy, never the real lake; prove CLI exit 1."""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import data_dir
from delta_crops.quality.report import write_report


def main() -> None:
    root = data_dir().resolve()
    source = root / "silver/ndvi/year=2023/month=07/date=2023-07-02.parquet"
    original_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    with TemporaryDirectory(prefix="delta-quality-corruption-") as temporary:
        copy = Path(temporary)
        shutil.copytree(root / "silver/ndvi", copy / "silver/ndvi")
        shutil.copy2(root / "silver/cells.parquet", copy / "silver/cells.parquet")
        (copy / "bronze/_logs").mkdir(parents=True)
        shutil.copy2(root / "bronze/_logs/ingestion_log.jsonl", copy / "bronze/_logs/ingestion_log.jsonl")
        target = copy / source.relative_to(root)
        table = pq.ParquetFile(target).read()
        index = table.schema.get_field_index("ndvi")
        values = table["ndvi"].to_pylist()
        values[0] = 1.25
        pq.write_table(table.set_column(index, table.schema.field(index), pa.array(values,type=pa.float32())),target)
        result = subprocess.run([sys.executable,"-m","delta_crops.quality.cli","--data-dir",str(copy)],
            capture_output=True,text=True)
        print(result.stdout, end="")
        print(f"Corruption CLI exit code: {result.returncode}")
        reports = list((copy / "silver/_reports/quality").glob("quality_*.json"))
        assert len(reports) == 1, result.stderr
        report = json.loads(reports[0].read_text())
        assert result.returncode == 10 and any(c["check"]=="ndvi_range" and c["count"]==1 for c in report["checks"])
        assert hashlib.sha256(source.read_bytes()).hexdigest() == original_hash
        report["acceptance_demo"] = dict(disposable_copy=True, ndvi_corruption=1.25,
            cli_exit_code=result.returncode, original_file=str(source), original_sha256=original_hash,
            original_unchanged=True, stdout=result.stdout)
        path = root / "silver/_reports/quality" / f"corruption_acceptance_{report['run_id']}.json"
        write_report(path,report)
        print(f"Original Silver unchanged. Acceptance report: {path}")


if __name__ == "__main__":
    main()
