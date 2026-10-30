"""
製造業 IoT デモ用テストデータ生成スクリプト

Usage:
    python generate_test_data.py                    # デフォルト設定で生成
    python generate_test_data.py --rows 1000        # 行数指定
    python generate_test_data.py --output ./output  # 出力先指定

生成ファイル:
    - production_normal.csv   正常データ（500行）
    - production_edge.csv     エッジケース（50行）
    - production_error.csv    異常データ（30行）
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)

# --- マスタデータ定義 ---
LINE_IDS = [f"LINE-{i:03d}" for i in range(1, 6)]
PRODUCT_CODES = [f"PRD-A{i:03d}" for i in range(1, 21)]
EQUIPMENT_IDS = [f"EQ-{i:03d}" for i in range(1, 11)]
OPERATOR_IDS = [f"OP-{i:03d}" for i in range(1, 51)]
SHIFTS = ["日勤", "夜勤"]
STATUSES = ["正常", "警告", "異常", "停止"]
STATUS_WEIGHTS = [0.75, 0.15, 0.07, 0.03]

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


def _random_status():
    return random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]


def _base_date(days_back=30):
    return datetime(2026, 3, 1) - timedelta(days=days_back)


def generate_normal_row(date):
    """正常データ 1 行を生成する。"""
    planned = random.randint(100, 1000)
    actual = max(0, planned + random.randint(-50, 50))
    defect = random.randint(0, min(50, actual))
    temperature = round(random.uniform(20.0, 80.0), 1)
    vibration = round(random.uniform(0.1, 5.0), 2)
    created_at = datetime.combine(date, datetime.min.time()) + timedelta(
        hours=random.randint(6, 22),
        minutes=random.randint(0, 59),
    )

    return {
        "production_date": date.strftime("%Y-%m-%d"),
        "line_id": random.choice(LINE_IDS),
        "product_code": random.choice(PRODUCT_CODES),
        "planned_quantity": planned,
        "actual_quantity": actual,
        "defect_quantity": defect,
        "equipment_id": random.choice(EQUIPMENT_IDS),
        "shift": random.choice(SHIFTS),
        "operator_id": random.choice(OPERATOR_IDS),
        "temperature": temperature,
        "vibration": vibration,
        "status": _random_status(),
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def generate_normal_data(num_rows=500):
    """正常な生産実績データを生成する。"""
    rows = []
    for i in range(num_rows):
        date = _base_date(30) + timedelta(days=i % 30)
        rows.append(generate_normal_row(date))
    return rows


def generate_edge_data(num_rows=50):
    """エッジケースデータを生成する（NULL, 境界値, Unicode）。"""
    rows = []
    base = _base_date(10)

    for i in range(num_rows):
        date = base + timedelta(days=i % 10)
        row = generate_normal_row(date)

        edge_type = i % 10
        if edge_type == 0:
            row["line_id"] = ""
        elif edge_type == 1:
            row["product_code"] = ""
        elif edge_type == 2:
            row["planned_quantity"] = 0
            row["actual_quantity"] = 0
            row["defect_quantity"] = 0
        elif edge_type == 3:
            row["planned_quantity"] = 99999
            row["actual_quantity"] = 99999
        elif edge_type == 4:
            row["temperature"] = 0.0
        elif edge_type == 5:
            row["temperature"] = 999.9
        elif edge_type == 6:
            row["vibration"] = 0.0
        elif edge_type == 7:
            row["operator_id"] = "OP-テスト太郎"
        elif edge_type == 8:
            row["status"] = ""
        elif edge_type == 9:
            row["created_at"] = ""

        rows.append(row)
    return rows


def generate_error_data(num_rows=30):
    """異常データを生成する（型不一致, 未来日付, スキーマ違反）。"""
    rows = []

    for i in range(num_rows):
        error_type = i % 6

        if error_type == 0:
            rows.append({
                "production_date": "not-a-date",
                "line_id": "LINE-001",
                "product_code": "PRD-A001",
                "planned_quantity": 100,
                "actual_quantity": 90,
                "defect_quantity": 5,
                "equipment_id": "EQ-001",
                "shift": "日勤",
                "operator_id": "OP-001",
                "temperature": 50.0,
                "vibration": 1.5,
                "status": "正常",
                "created_at": "2026-03-01 10:00:00",
            })
        elif error_type == 1:
            rows.append({
                "production_date": "2099-12-31",
                "line_id": "LINE-001",
                "product_code": "PRD-A001",
                "planned_quantity": 100,
                "actual_quantity": 90,
                "defect_quantity": 5,
                "equipment_id": "EQ-001",
                "shift": "日勤",
                "operator_id": "OP-001",
                "temperature": 50.0,
                "vibration": 1.5,
                "status": "正常",
                "created_at": "2099-12-31 10:00:00",
            })
        elif error_type == 2:
            rows.append({
                "production_date": "2026-03-01",
                "line_id": "LINE-001",
                "product_code": "PRD-A001",
                "planned_quantity": "abc",
                "actual_quantity": "xyz",
                "defect_quantity": 5,
                "equipment_id": "EQ-001",
                "shift": "日勤",
                "operator_id": "OP-001",
                "temperature": 50.0,
                "vibration": 1.5,
                "status": "正常",
                "created_at": "2026-03-01 10:00:00",
            })
        elif error_type == 3:
            rows.append({
                "production_date": "2026-03-01",
                "line_id": "LINE-001",
                "product_code": "PRD-A001",
                "planned_quantity": -100,
                "actual_quantity": -50,
                "defect_quantity": -10,
                "equipment_id": "EQ-001",
                "shift": "日勤",
                "operator_id": "OP-001",
                "temperature": -999.0,
                "vibration": -1.0,
                "status": "正常",
                "created_at": "2026-03-01 10:00:00",
            })
        elif error_type == 4:
            rows.append({
                "production_date": "2026-03-01",
                "line_id": "LINE-001",
                "product_code": "PRD-A001",
                "planned_quantity": 100,
                "actual_quantity": 90,
                "defect_quantity": 5,
                "equipment_id": "EQ-001",
                "shift": "不明なシフト",
                "operator_id": "OP-001",
                "temperature": 50.0,
                "vibration": 1.5,
                "status": "不明なステータス",
                "created_at": "2026-03-01 10:00:00",
            })
        elif error_type == 5:
            rows.append({
                "production_date": "2026-03-01",
                "line_id": "",
                "product_code": "",
                "planned_quantity": "",
                "actual_quantity": "",
                "defect_quantity": "",
                "equipment_id": "",
                "shift": "",
                "operator_id": "",
                "temperature": "",
                "vibration": "",
                "status": "",
                "created_at": "",
            })

    return rows


def write_csv(rows, filepath):
    """CSV ファイルに書き出す。"""
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {filepath.name}: {len(rows)} rows")


def main():
    parser = argparse.ArgumentParser(description="製造業 IoT デモ用テストデータ生成")
    parser.add_argument("--rows", type=int, default=500, help="正常データの行数")
    parser.add_argument("--output", type=str, default=".", help="出力先ディレクトリ")
    args = parser.parse_args()

    output_dir = Path(args.output)
    print(f"テストデータ生成 (seed={SEED})")
    print(f"出力先: {output_dir.resolve()}")

    write_csv(generate_normal_data(args.rows), output_dir / "production_normal.csv")
    write_csv(generate_edge_data(50), output_dir / "production_edge.csv")
    write_csv(generate_error_data(30), output_dir / "production_error.csv")

    print("完了!")


if __name__ == "__main__":
    main()
