"""Запись сгенерированных данных в Unity Catalog Volume.

Volume монтируется как обычный путь `/Volumes/<catalog>/<schema>/<volume>/...`,
поэтому пишем стандартным `open()` — никакого Spark для этого не нужно.
Формат специально разный, чтобы Auto Loader потренировался на всех:

* orders / order_items — JSON Lines
* customers            — CSV с заголовком
* products             — JSON Lines
* clickstream          — JSON Lines маленькими файлами (микробатчи)
"""

from __future__ import annotations

import csv
import io
import json
import os
from collections.abc import Iterable, Sequence


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def write_jsonl(path: str, records: Iterable[dict]) -> int:
    ensure_dir(os.path.dirname(path))
    n = 0
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            n += 1
    return n


def write_csv(path: str, records: Sequence[dict], fieldnames: Sequence[str] | None = None) -> int:
    if not records:
        return 0
    ensure_dir(os.path.dirname(path))
    fieldnames = list(fieldnames or records[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())
    return len(records)


def write_chunks(
    directory: str,
    records: Sequence[dict],
    prefix: str,
    chunk_size: int = 500,
) -> list[str]:
    """Разложить записи по нескольким файлам — так стрим видит несколько батчей."""
    ensure_dir(directory)
    paths = []
    for idx in range(0, len(records), chunk_size):
        path = os.path.join(directory, f"{prefix}_{idx // chunk_size:04d}.json")
        write_jsonl(path, records[idx : idx + chunk_size])
        paths.append(path)
    return paths
