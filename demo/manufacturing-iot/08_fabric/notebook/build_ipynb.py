"""
fabric_pipeline.py（`# %%` セル区切り）→ fabric_pipeline.ipynb 変換スクリプト

Fabric のワークスペースに .ipynb としてインポートするために使う。
`# %% [markdown]` セルはマークダウンセルに、`# %% [parameters]` セルは
`parameters` タグ付きのコードセル（パラメーターセル）に変換する。

Usage:
    python build_ipynb.py            # 同じフォルダに fabric_pipeline.ipynb を出力
"""

import json
from pathlib import Path

SOURCE = Path(__file__).with_name("fabric_pipeline.py")
TARGET = Path(__file__).with_name("fabric_pipeline.ipynb")
MARKER = "# %%"


def split_cells(text: str) -> list[tuple[str, list[str]]]:
    """`# %%` 区切りで (種別, 行リスト) に分割する。"""
    cells: list[tuple[str, list[str]]] = []
    kind, lines = None, []
    for line in text.splitlines():
        if line.startswith(MARKER):
            if kind is not None:
                cells.append((kind, lines))
            tag = line[len(MARKER):].strip()
            kind = {"[markdown]": "markdown", "[parameters]": "parameters"}.get(tag, "code")
            lines = []
        else:
            lines.append(line)
    if kind is not None:
        cells.append((kind, lines))
    return cells


def _trim(lines: list[str]) -> list[str]:
    start, end = 0, len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def to_notebook_cell(kind: str, lines: list[str]) -> dict:
    body = _trim(lines)
    if kind == "markdown":
        body = [line[2:] if line.startswith("# ") else line.lstrip("#") for line in body]
    source = [line + "\n" for line in body]
    if source:
        source[-1] = source[-1].rstrip("\n")

    if kind == "markdown":
        return {"cell_type": "markdown", "metadata": {}, "source": source}
    metadata = {"tags": ["parameters"]} if kind == "parameters" else {}
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": metadata,
        "outputs": [],
        "source": source,
    }


def build() -> dict:
    cells = [to_notebook_cell(kind, lines) for kind, lines in split_cells(SOURCE.read_text(encoding="utf-8"))]
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "PySpark", "language": "python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    TARGET.write_text(json.dumps(build(), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"written: {TARGET.name}")


if __name__ == "__main__":
    main()
