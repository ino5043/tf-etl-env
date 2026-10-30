"""
Fabric デモ用サンプルデータ生成スクリプト（ダッシュボード向け 30 日分）

06_testing/sample_data/generate_test_data.py のカラム定義・値ドメインを踏襲し、
ダッシュボードで「傾向」が見えるよう以下の特徴を持たせた日次 MES CSV を生成する。

- 1 ファイル = 1 日（production_YYYYMMDD.csv）。固定シードで再現可能
- 設備はラインに固定配置（各ライン 2 台）。製品もラインごとに 4 品目を担当
- 劣化トレンド:
    EQ-007（LINE-004）: 温度が徐々に上昇（7 日平均 > 65℃ で温度アラート）
    EQ-003（LINE-002）: 振動が徐々に増加（7 日平均 > 3.5 mm/s で振動アラート）
  劣化設備は終盤に警告/異常ステータス・不良率・達成率悪化が増える
- 品質不良（defect_quantity）は通常 1〜2% 程度。劣化設備の終盤と品質トラブル日
  （2026-10-17 の LINE-003）はライン日次の不良率が 5% を超える
- データ品質エラー（Dead Letter 行き）を数日に 1 行だけ混入
- MES の再送を模した重複レコードを数日に 1 行混入（Silver で重複排除される）

Usage:
    python generate_fabric_sample.py                       # 2026-10-01 から 30 日分
    python generate_fabric_sample.py --start 2026-11-01 --days 14 --output ./out

生成ファイル:
    production_YYYYMMDD.csv   日次 MES 生産実績（UTF-8 BOM 付き）
    line_master.csv           ラインマスタ
    product_master.csv        製品マスタ
    equipment_master_seed.csv 設備マスタ（初期値）
"""

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 20261001
DEFAULT_START = "2026-10-01"
DEFAULT_DAYS = 30

# 06_testing/sample_data/generate_test_data.py と同一のカラム順
COLUMNS = [
    "production_date",
    "line_id",
    "product_code",
    "planned_quantity",
    "actual_quantity",
    "defect_quantity",
    "equipment_id",
    "shift",
    "operator_id",
    "temperature",
    "vibration",
    "status",
    "created_at",
]

SHIFTS = ["日勤", "夜勤"]
SHIFT_HOURS = {"日勤": (8, 16), "夜勤": (20, 23)}

# --- マスタ定義 ---------------------------------------------------------------
# plant_id は RLS（08_fabric/design/security-entra-id.md）で工場単位に絞るために使う
PLANTS = {"PLANT-A": "A工場", "PLANT-B": "B工場"}
LINES = [
    # line_id, line_name, plant_id, process, capacity_per_shift
    ("LINE-001", "第1組立ライン", "PLANT-A", "組立", 3200),
    ("LINE-002", "第2組立ライン", "PLANT-A", "組立", 3000),
    ("LINE-003", "プレスライン", "PLANT-A", "プレス", 3600),
    ("LINE-004", "成形ライン", "PLANT-B", "樹脂成形", 2800),
    ("LINE-005", "検査・梱包ライン", "PLANT-B", "検査梱包", 3400),
]

PRODUCT_CATEGORIES = {
    "LINE-001": ("ユニット", ["制御ユニットA", "制御ユニットB", "電源ユニット", "センサーユニット"]),
    "LINE-002": ("ユニット", ["駆動ユニットA", "駆動ユニットB", "ギアボックス", "モーターASSY"]),
    "LINE-003": ("板金部品", ["ブラケットS", "ブラケットL", "カバーパネル", "ベースプレート"]),
    "LINE-004": ("樹脂部品", ["ハウジング上", "ハウジング下", "コネクタケース", "ノブ"]),
    "LINE-005": ("完成品", ["完成品X", "完成品Y", "保守部品セット", "輸出仕様品"]),
}

EQUIPMENT_TYPES = {
    "LINE-001": ("組立ロボット", "ねじ締め機"),
    "LINE-002": ("組立ロボット", "圧入機"),
    "LINE-003": ("プレス機", "レベラー"),
    "LINE-004": ("射出成形機", "金型温調機"),
    "LINE-005": ("画像検査機", "自動梱包機"),
}

# 設備の基準値（温度 ℃, 振動 mm/s）
EQUIPMENT_BASELINE = {
    "EQ-001": (42.0, 1.2), "EQ-002": (38.0, 1.0),
    "EQ-003": (45.0, 1.5), "EQ-004": (40.0, 1.3),
    "EQ-005": (52.0, 2.0), "EQ-006": (36.0, 0.9),
    "EQ-007": (48.0, 1.6), "EQ-008": (44.0, 0.8),
    "EQ-009": (35.0, 0.6), "EQ-010": (37.0, 1.1),
}

# 劣化トレンド: 期間終了時点での上昇幅（温度 ℃, 振動 mm/s）
DEGRADATION = {
    "EQ-007": (23.0, 0.6),   # 温度上昇型（射出成形機）
    "EQ-003": (3.0, 2.6),    # 振動増加型（組立ロボット）
}

TEMP_RANGE = (20.0, 80.0)    # transform.validate_quality の許容範囲
VIB_RANGE = (0.1, 5.0)

# データ品質エラーの注入（day_index → 種別）。Dead Letter のデモ用
ERROR_INJECTIONS = {
    7: "invalid_status",
    14: "missing_product_code",
    21: "defect_gt_actual",
    26: "non_numeric_quantity",
}
# 再送重複の注入日（day_index）
DUPLICATE_DAYS = {10, 24}
# 品質トラブル日（day_index → line_id）: 材料ロット不良を模して不良率を一時的に引き上げる
QUALITY_INCIDENTS = {16: "LINE-003"}
QUALITY_INCIDENT_DEFECT_RATIO = 0.08


def build_masters() -> dict[str, list[dict]]:
    """Power BI 用のマスタ CSV 行を構築する。"""
    line_rows = [
        {
            "line_id": line_id,
            "line_name": name,
            "plant_id": plant_id,
            "plant_name": PLANTS[plant_id],
            "process": process,
            "capacity_per_shift": capacity,
        }
        for line_id, name, plant_id, process, capacity in LINES
    ]

    product_rows = []
    equipment_rows = []
    for line_index, (line_id, *_rest) in enumerate(LINES):
        category, names = PRODUCT_CATEGORIES[line_id]
        for offset, product_name in enumerate(names):
            number = line_index * 4 + offset + 1
            product_rows.append({
                "product_code": f"PRD-A{number:03d}",
                "product_name": product_name,
                "category": category,
                "main_line_id": line_id,
                "unit": "個",
                "standard_cycle_time_sec": 20 + (number * 7) % 40,
            })
        for offset, equipment_type in enumerate(EQUIPMENT_TYPES[line_id]):
            number = line_index * 2 + offset + 1
            equipment_id = f"EQ-{number:03d}"
            temp, vib = EQUIPMENT_BASELINE[equipment_id]
            equipment_rows.append({
                "equipment_id": equipment_id,
                "equipment_name": f"{equipment_type}{number:02d}",
                "equipment_type": equipment_type,
                "line_id": line_id,
                "install_date": f"{2015 + number % 8}-04-01",
                "rated_temperature_c": 80.0,
                "baseline_temperature_c": temp,
                "baseline_vibration_mm_s": vib,
            })

    return {
        "line_master.csv": line_rows,
        "product_master.csv": product_rows,
        "equipment_master_seed.csv": equipment_rows,
    }


def _clip(value: float, bounds: tuple[float, float]) -> float:
    low, high = bounds
    return min(max(value, low), high)


def _status(rng: random.Random, progress: float) -> str:
    """設備状態を劣化進行度（0.0〜1.0）に応じて抽選する。"""
    warn = 0.03 + 0.30 * progress ** 2
    abnormal = 0.005 + 0.10 * progress ** 3
    stop = 0.002 + 0.03 * progress ** 3
    roll = rng.random()
    if roll < stop:
        return "停止"
    if roll < stop + abnormal:
        return "異常"
    if roll < stop + abnormal + warn:
        return "警告"
    return "正常"


def _production_row(
    rng: random.Random,
    day: date,
    day_index: int,
    total_days: int,
    line_id: str,
    product_code: str,
    equipment_id: str,
    shift: str,
) -> dict:
    """1 生産ロット（日 × ライン × 製品 × シフト）の実績行を生成する。"""
    degradation = DEGRADATION.get(equipment_id)
    progress = (day_index / max(total_days - 1, 1)) if degradation else 0.0
    temp_base, vib_base = EQUIPMENT_BASELINE[equipment_id]
    temp_drift, vib_drift = degradation if degradation else (0.0, 0.0)

    temperature = _clip(rng.gauss(temp_base + temp_drift * progress ** 1.5, 2.0), TEMP_RANGE)
    vibration = _clip(rng.gauss(vib_base + vib_drift * progress ** 1.5, 0.18), VIB_RANGE)

    planned = rng.randrange(300, 820, 10)
    achievement = rng.gauss(0.98 - 0.12 * progress ** 2, 0.03)
    actual = max(0, int(planned * min(achievement, 1.08)))
    mean_defect = 0.012 + 0.07 * progress ** 2
    if QUALITY_INCIDENTS.get(day_index) == line_id:
        mean_defect = QUALITY_INCIDENT_DEFECT_RATIO
    defect_ratio = max(0.0, rng.gauss(mean_defect, 0.006))
    defect = min(actual, int(round(actual * defect_ratio)))

    start_hour, end_hour = SHIFT_HOURS[shift]
    created_at = datetime.combine(day, datetime.min.time()) + timedelta(
        hours=rng.randint(start_hour, end_hour),
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
    )

    # センサー欠損（約 0.5%）は NULL（空文字）として出力
    temperature_text = "" if rng.random() < 0.005 else f"{temperature:.1f}"

    return {
        "production_date": day.isoformat(),
        "line_id": line_id,
        "product_code": product_code,
        "planned_quantity": planned,
        "actual_quantity": actual,
        "defect_quantity": defect,
        "equipment_id": equipment_id,
        "shift": shift,
        "operator_id": f"OP-{rng.randint(1, 50):03d}",
        "temperature": temperature_text,
        "vibration": f"{vibration:.2f}",
        "status": _status(rng, progress),
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _inject_error(row: dict, error_type: str) -> dict:
    """品質ゲートで不合格になる行を作る（元の行は変更しない）。"""
    broken = dict(row)
    if error_type == "invalid_status":
        broken["status"] = "不明"
    elif error_type == "missing_product_code":
        broken["product_code"] = ""
    elif error_type == "defect_gt_actual":
        broken["defect_quantity"] = int(broken["actual_quantity"]) + 15
    elif error_type == "non_numeric_quantity":
        broken["planned_quantity"] = "N/A"
    return broken


def _resend(row: dict) -> dict:
    """MES の再送（数量訂正 + created_at が後）を模した重複行を作る。"""
    resent = dict(row)
    created = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S")
    resent["created_at"] = (created + timedelta(minutes=45)).strftime("%Y-%m-%d %H:%M:%S")
    resent["actual_quantity"] = int(row["actual_quantity"]) + 5
    return resent


def generate_day(
    rng: random.Random,
    day: date,
    day_index: int,
    total_days: int,
    masters: dict[str, list[dict]],
) -> list[dict]:
    """1 日分の生産実績行を生成する。"""
    products_by_line: dict[str, list[str]] = {}
    for product in masters["product_master.csv"]:
        products_by_line.setdefault(product["main_line_id"], []).append(product["product_code"])
    equipment_by_line: dict[str, list[str]] = {}
    for equipment in masters["equipment_master_seed.csv"]:
        equipment_by_line.setdefault(equipment["line_id"], []).append(equipment["equipment_id"])

    # 日曜日は日勤のみ稼働
    shifts = ["日勤"] if day.weekday() == 6 else SHIFTS

    rows = []
    for line_id, products in products_by_line.items():
        equipments = equipment_by_line[line_id]
        for shift in shifts:
            for index, product_code in enumerate(products):
                if rng.random() < 0.08:  # 段取り替え等で当該ロットなし
                    continue
                equipment_id = equipments[index % len(equipments)]
                rows.append(_production_row(
                    rng, day, day_index, total_days,
                    line_id, product_code, equipment_id, shift,
                ))

    if day_index in DUPLICATE_DAYS and rows:
        rows.append(_resend(rows[len(rows) // 2]))
    if day_index in ERROR_INJECTIONS and rows:
        target = len(rows) // 3
        rows[target] = _inject_error(rows[target], ERROR_INJECTIONS[day_index])

    return sorted(rows, key=lambda r: r["created_at"])


def write_csv(rows: list[dict], filepath: Path, fieldnames: list[str], bom: bool = True) -> None:
    """CSV を書き出す（MES 出力に合わせ UTF-8 BOM 付き）。"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    encoding = "utf-8-sig" if bom else "utf-8"
    with open(filepath, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate(output_dir: Path, start: date, days: int, seed: int = SEED) -> dict:
    """サンプルデータ一式を生成し、件数サマリを返す。"""
    rng = random.Random(seed)
    masters = build_masters()

    for filename, rows in masters.items():
        write_csv(rows, output_dir / filename, list(rows[0].keys()), bom=False)

    daily_counts = {}
    for day_index in range(days):
        day = start + timedelta(days=day_index)
        rows = generate_day(rng, day, day_index, days, masters)
        write_csv(rows, output_dir / f"production_{day:%Y%m%d}.csv", COLUMNS)
        daily_counts[day.isoformat()] = len(rows)

    return {
        "days": days,
        "total_rows": sum(daily_counts.values()),
        "daily_counts": daily_counts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fabric デモ用 30 日分サンプルデータ生成")
    parser.add_argument("--start", default=DEFAULT_START, help="開始日（YYYY-MM-DD）")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="日数")
    parser.add_argument("--seed", type=int, default=SEED, help="乱数シード")
    parser.add_argument(
        "--output", default=str(Path(__file__).parent), help="出力先ディレクトリ",
    )
    args = parser.parse_args()

    summary = generate(
        Path(args.output),
        date.fromisoformat(args.start),
        args.days,
        args.seed,
    )
    print(f"seed={args.seed} 出力先={Path(args.output).resolve()}")
    print(f"日数={summary['days']} 総行数={summary['total_rows']}")


if __name__ == "__main__":
    main()
