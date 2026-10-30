# DAX メジャー定義 — 製造業 IoT デモ

| 項目 | 値 |
|------|-----|
| 対象モデル | `semantic-model.md` のセマンティックモデル（Direct Lake） |
| メジャー数 | 80（任意の明細メジャー 4 件を除く） |
| ホームテーブル | 生産・品質・共通・目標・書式 → `daily_production_kpi`、設備 → `equipment_health` |
| 一括追加用 | `dax-measures.dax`（DAX クエリ ビューで実行し、モデルに追加する） |

---

## 1. 設計ルール

| ルール | 内容 |
|--------|------|
| 比率は合計から再計算する | `達成率 = DIVIDE(SUM(実績), SUM(計画))`。Gold の `achievement_rate` 等の事前計算列は使わない |
| 割り算は `DIVIDE` | 分母 0 / 空白で `BLANK()` を返し、エラーや無限大を出さない |
| タイムインテリジェンス | `dim_date` を日付テーブルとしてマークしたうえで `DATEADD` / `DATESINPERIOD` を使う |
| 「最終日」系メジャー | カードは期間スライサーの最終データ日（= 朝会で見る「前日」）の値を表示する。期間全体のトレンドと同じページに置ける |
| 閾値はメジャーで一元管理 | 条件付き書式・定数線はすべて `09_目標・閾値` のメジャーを参照する。閾値変更は 1 か所で済む |
| 色はメジャーで一元管理 | 条件付き書式の「フィールド値」に `09_書式` の色メジャーを指定する。HEX 値は `_色` メジャー 3 つにだけ書く |

**比率を平均してはいけない理由:** 行ごとの比率を平均すると、計画規模の小さいラインと大きいラインが同じ重みで扱われ、工場全体の実際の達成率と一致しない。

| ライン | 計画数 | 実績数 | 行の達成率 |
|--------|-------|-------|-----------|
| ライン A | 1,000 | 900 | 90.0% |
| ライン B | 100 | 120 | 120.0% |
| 行比率の単純平均（誤り） | — | — | 105.0% |
| 合計から再計算（正しい） | 1,100 | 1,020 | **92.7%** |

`monitoring-design.md` 3.1 の `AVG(achievement_rate)` はパイプライン監視用の参考値であり、ダッシュボードの KPI とは定義が異なる。

---

## 2. 表示フォルダー一覧

| 表示フォルダー | ホームテーブル | 内容 |
|---------------|---------------|------|
| `00_共通` | `daily_production_kpi` | 最終日・データ鮮度 |
| `01_生産` | `daily_production_kpi` | 計画数・実績数・達成率 |
| `01_生産\時系列` | `daily_production_kpi` | 前日比・前週比・7 日移動平均 |
| `01_生産\最終日` | `daily_production_kpi` | カード用（最終データ日の値） |
| `02_品質` | `daily_production_kpi` | 不良数・不良率・歩留まり率 |
| `02_品質\最終日` | `daily_production_kpi` | カード用 |
| `03_設備` | `equipment_health` | 計測数・異常回数・簡易稼働率・アラート設備数・温度振動 |
| `03_設備\最終日` | `equipment_health` | カード用 |
| `09_目標・閾値` | `daily_production_kpi` | 条件付き書式・定数線の閾値 |
| `09_書式` | `daily_production_kpi` | 条件付き書式用の色 |

---

## 3. 共通・生産

### 3.1 `00_共通`

| メジャー | 書式 | 説明 |
|---------|------|------|
| `最終日` | `yyyy/mm/dd` | 現在の期間フィルター内で生産 KPI が存在する最後の日。ライン選択の影響を受けない |
| `最終集計日時` | `yyyy/mm/dd hh:nn` | Gold の最新集計日時（UTC）。フィルターの影響を受けない |
| `最終集計日時 (JST)` | `yyyy/mm/dd hh:nn` | 上記を日本時間に変換した値。データ鮮度カードに使う |

```dax
最終日 =
CALCULATE (
    MAX ( daily_production_kpi[production_date] ),
    REMOVEFILTERS ( dim_line )
)

最終集計日時 =
CALCULATE (
    MAX ( daily_production_kpi[_aggregated_at] ),
    REMOVEFILTERS ( dim_date ),
    REMOVEFILTERS ( dim_line )
)

最終集計日時 (JST) =
VAR _utc = [最終集計日時]
RETURN
    IF ( NOT ISBLANK ( _utc ), _utc + TIME ( 9, 0, 0 ) )
```

### 3.2 `01_生産`

| メジャー | 書式 | 説明 |
|---------|------|------|
| `計画数` | `#,0` | 計画数量の合計 |
| `実績数` | `#,0` | 実績数量の合計 |
| `良品数` | `#,0` | 良品数量の合計（実績 − 不良） |
| `日勤実績数` | `#,0` | 日勤の実績数量 |
| `夜勤実績数` | `#,0` | 夜勤の実績数量 |
| `計画差異` | `+#,0;-#,0;0` | 実績数 − 計画数 |
| `達成率` | `0.0%` | 実績数 ÷ 計画数（合計から再計算） |
| `夜勤比率` | `0.0%` | 夜勤実績数 ÷ 実績数 |

```dax
計画数 =
SUM ( daily_production_kpi[total_planned_quantity] )

実績数 =
SUM ( daily_production_kpi[total_actual_quantity] )

良品数 =
SUM ( daily_production_kpi[total_good_quantity] )

日勤実績数 =
SUM ( daily_production_kpi[day_shift_quantity] )

夜勤実績数 =
SUM ( daily_production_kpi[night_shift_quantity] )

計画差異 =
VAR _plan = [計画数]
VAR _actual = [実績数]
RETURN
    IF ( NOT ISBLANK ( _plan ), _actual - _plan )

達成率 =
DIVIDE ( [実績数], [計画数] )

夜勤比率 =
DIVIDE ( [夜勤実績数], [実績数] )
```

### 3.3 `01_生産\時系列`

| メジャー | 書式 | 説明 |
|---------|------|------|
| `実績数 前日` | `#,0` | 暦日の前日の実績数 |
| `実績数 前日比` | `+0.0%;-0.0%;0.0%` | （当日 − 前日）÷ 前日。どちらかが空白なら空白 |
| `実績数 前週同日` | `#,0` | 7 日前の実績数 |
| `実績数 前週比` | `+0.0%;-0.0%;0.0%` | （当日 − 7 日前）÷ 7 日前 |
| `達成率 前日差` | `+0.0" pt";-0.0" pt";0.0" pt"` | 達成率の前日との差（ポイント） |
| `実績数 7日移動平均` | `#,0` | 直近 7 日間の 1 日あたり実績数（データのない日は平均から除外） |
| `達成率 7日移動` | `0.0%` | 直近 7 日間の実績合計 ÷ 計画合計 |

```dax
実績数 前日 =
CALCULATE ( [実績数], DATEADD ( dim_date[calendar_date], -1, DAY ) )

実績数 前日比 =
VAR _cur = [実績数]
VAR _prev = [実績数 前日]
RETURN
    IF (
        NOT ISBLANK ( _cur ) && NOT ISBLANK ( _prev ),
        DIVIDE ( _cur - _prev, _prev )
    )

実績数 前週同日 =
CALCULATE ( [実績数], DATEADD ( dim_date[calendar_date], -7, DAY ) )

実績数 前週比 =
VAR _cur = [実績数]
VAR _prev = [実績数 前週同日]
RETURN
    IF (
        NOT ISBLANK ( _cur ) && NOT ISBLANK ( _prev ),
        DIVIDE ( _cur - _prev, _prev )
    )

達成率 前日差 =
VAR _cur = [達成率]
VAR _prev =
    CALCULATE ( [達成率], DATEADD ( dim_date[calendar_date], -1, DAY ) )
RETURN
    IF (
        NOT ISBLANK ( _cur ) && NOT ISBLANK ( _prev ),
        ( _cur - _prev ) * 100
    )

実績数 7日移動平均 =
VAR _end = MAX ( dim_date[calendar_date] )
VAR _result =
    CALCULATE (
        AVERAGEX ( VALUES ( dim_date[calendar_date] ), [実績数] ),
        DATESINPERIOD ( dim_date[calendar_date], _end, -7, DAY )
    )
RETURN
    IF ( NOT ISBLANK ( [実績数] ), _result )

達成率 7日移動 =
VAR _end = MAX ( dim_date[calendar_date] )
VAR _result =
    CALCULATE (
        [達成率],
        DATESINPERIOD ( dim_date[calendar_date], _end, -7, DAY )
    )
RETURN
    IF ( NOT ISBLANK ( [実績数] ), _result )
```

- 7 日移動系は日付軸のビジュアルで使う。末尾の `IF ( NOT ISBLANK ( [実績数] ), … )` により、データのない日（データ範囲の後ろの日付など）に線が伸びない
- `DATESINPERIOD` は期間スライサーの開始日より前の日も含めて計算する（スライサー開始直後から正しい移動平均が出る）
- `AVERAGEX` は空白を平均から除外するため、`実績数 7日移動平均` は「稼働日平均」になる

### 3.4 `01_生産\最終日`（カード用）

| メジャー | 書式 | 説明 |
|---------|------|------|
| `計画数 (最終日)` | `#,0` | 最終日の計画数 |
| `実績数 (最終日)` | `#,0` | 最終日の実績数 |
| `計画差異 (最終日)` | `+#,0;-#,0;0` | 最終日の計画差異 |
| `達成率 (最終日)` | `0.0%` | 最終日の達成率 |
| `実績数 前日比 (最終日)` | `+0.0%;-0.0%;0.0%` | 最終日の実績数の前日比 |
| `達成率 前日差 (最終日)` | `+0.0" pt";-0.0" pt";0.0" pt"` | 最終日の達成率の前日差 |
| `実績数 7日移動平均 (最終日)` | `#,0` | 最終日時点の 7 日移動平均 |

```dax
計画数 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [計画数], dim_date[calendar_date] = _d )

実績数 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [実績数], dim_date[calendar_date] = _d )

計画差異 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [計画差異], dim_date[calendar_date] = _d )

達成率 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [達成率], dim_date[calendar_date] = _d )

実績数 前日比 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [実績数 前日比], dim_date[calendar_date] = _d )

達成率 前日差 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [達成率 前日差], dim_date[calendar_date] = _d )

実績数 7日移動平均 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [実績数 7日移動平均], dim_date[calendar_date] = _d )
```

`_d` は変数で先に評価してから `CALCULATE` のフィルターに渡す。日付列へのフィルターは日付テーブルのマークにより `dim_date` 全体のフィルターを置き換えるため、年月スライサーなどと併用しても最終日 1 日だけに絞られる。

---

## 4. 品質

### 4.1 `02_品質`

| メジャー | 書式 | 説明 |
|---------|------|------|
| `不良数` | `#,0` | 不良数量の合計 |
| `不良率` | `0.00%` | 不良数 ÷ 実績数（合計から再計算） |
| `歩留まり率` | `0.00%` | 良品数 ÷ 実績数（合計から再計算） |
| `不良率 前日差` | `+0.00" pt";-0.00" pt";0.00" pt"` | 不良率の前日との差（ポイント） |
| `不良率 7日移動` | `0.00%` | 直近 7 日間の不良合計 ÷ 実績合計 |
| `不良率 目標差` | `+0.00" pt";-0.00" pt";0.00" pt"` | 不良率 − 不良率 目標（正 = 目標超過） |
| `不良率 目標超過ライン数` | `#,0` | 現在のフィルター内で不良率が目標を超えたライン数 |

```dax
不良数 =
SUM ( daily_production_kpi[total_defect_quantity] )

不良率 =
DIVIDE ( [不良数], [実績数] )

歩留まり率 =
DIVIDE ( [良品数], [実績数] )

不良率 前日差 =
VAR _cur = [不良率]
VAR _prev =
    CALCULATE ( [不良率], DATEADD ( dim_date[calendar_date], -1, DAY ) )
RETURN
    IF (
        NOT ISBLANK ( _cur ) && NOT ISBLANK ( _prev ),
        ( _cur - _prev ) * 100
    )

不良率 7日移動 =
VAR _end = MAX ( dim_date[calendar_date] )
VAR _result =
    CALCULATE (
        [不良率],
        DATESINPERIOD ( dim_date[calendar_date], _end, -7, DAY )
    )
RETURN
    IF ( NOT ISBLANK ( [実績数] ), _result )

不良率 目標差 =
VAR _v = [不良率]
RETURN
    IF ( NOT ISBLANK ( _v ), ( _v - [不良率 目標] ) * 100 )

不良率 目標超過ライン数 =
COALESCE (
    COUNTROWS (
        FILTER ( VALUES ( dim_line[line_id] ), [不良率] > [不良率 目標] )
    ),
    0
)
```

不良率は 5% 前後の小さい値を扱うため、小数第 2 位まで表示する。

### 4.2 `02_品質\最終日`（カード用）

| メジャー | 書式 | 説明 |
|---------|------|------|
| `不良数 (最終日)` | `#,0` | 最終日の不良数 |
| `不良率 (最終日)` | `0.00%` | 最終日の不良率 |
| `歩留まり率 (最終日)` | `0.00%` | 最終日の歩留まり率 |
| `不良率 前日差 (最終日)` | `+0.00" pt";-0.00" pt";0.00" pt"` | 最終日の不良率の前日差 |
| `不良率 目標差 (最終日)` | `+0.00" pt";-0.00" pt";0.00" pt"` | 最終日の不良率の目標差 |
| `不良率 目標超過ライン数 (最終日)` | `#,0` | 最終日に不良率が目標を超えたライン数 |

```dax
不良数 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [不良数], dim_date[calendar_date] = _d )

不良率 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [不良率], dim_date[calendar_date] = _d )

歩留まり率 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [歩留まり率], dim_date[calendar_date] = _d )

不良率 前日差 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [不良率 前日差], dim_date[calendar_date] = _d )

不良率 目標差 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [不良率 目標差], dim_date[calendar_date] = _d )

不良率 目標超過ライン数 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [不良率 目標超過ライン数], dim_date[calendar_date] = _d )
```

---

## 5. 設備

### 5.1 稼働率の定義（重要）

本デモのデータには**稼働時間・停止時間・サイクルタイムがない**。`equipment_health` が持つのは「計測レコード数」と「異常/停止状態だったレコード数」だけである。そのため、本デモの稼働率は次の**簡易指標**として定義し、名称にも「簡易」を付ける。

| 指標 | 定義 | 本デモで算出可否 |
|------|------|-----------------|
| 正常稼働率（簡易） | （計測レコード数 − 異常/停止レコード数）÷ 計測レコード数。`警告` は稼働中として数える | ○ |
| 時間稼働率（可用率） | 稼働時間 ÷ 負荷時間 | ×（稼働・停止時刻のデータが必要） |
| 性能稼働率 | （理論サイクルタイム × 加工数）÷ 稼働時間 | ×（サイクルタイムが必要） |
| 良品率 | 良品数 ÷ 加工数 | ○（`歩留まり率` で代替。ただしライン粒度） |
| OEE（設備総合効率） | 時間稼働率 × 性能稼働率 × 良品率 | ×（上 2 つが算出できない） |

正常稼働率（簡易）は「計測された時点のうち設備が正常/警告状態だった割合」であり、時間の比率ではない。デモでは「本番では MES / PLC の稼働ログを追加して OEE に拡張する」ことを次のステップとして示す。

### 5.2 `03_設備`

| メジャー | 書式 | 説明 |
|---------|------|------|
| `計測数` | `#,0` | 計測レコード数の合計 |
| `異常発生回数` | `#,0` | 異常/停止状態だったレコード数の合計 |
| `正常稼働率 (簡易)` | `0.0%` | （計測数 − 異常発生回数）÷ 計測数。5.1 参照 |
| `監視設備数` | `#,0` | データのある設備数 |
| `温度アラート設備数` | `#,0` | 温度アラート（7 日平均 > 65℃）が 1 回以上出た設備数 |
| `振動アラート設備数` | `#,0` | 振動アラート（7 日平均 > 3.5 mm/s）が 1 回以上出た設備数 |
| `アラート設備数` | `#,0` | 温度または振動アラートが 1 回以上出た設備数 |
| `異常発生設備数` | `#,0` | 異常/停止が 1 回以上発生した設備数 |
| `平均温度 (7日)` | `0.0" ℃"` | 7 日移動平均温度 |
| `最高温度` | `0.0" ℃"` | 当日最高温度の最大値 |
| `平均振動 (7日)` | `0.00" mm/s"` | 7 日移動平均振動 |
| `最大振動` | `0.00" mm/s"` | 当日最大振動の最大値 |

```dax
計測数 =
SUM ( equipment_health[measurement_count] )

異常発生回数 =
SUM ( equipment_health[abnormal_count] )

正常稼働率 (簡易) =
VAR _total = [計測数]
RETURN
    DIVIDE ( _total - [異常発生回数], _total )

監視設備数 =
DISTINCTCOUNT ( equipment_health[equipment_id] )

温度アラート設備数 =
CALCULATE (
    DISTINCTCOUNT ( equipment_health[equipment_id] ),
    KEEPFILTERS ( equipment_health[temperature_alert] = TRUE () )
)

振動アラート設備数 =
CALCULATE (
    DISTINCTCOUNT ( equipment_health[equipment_id] ),
    KEEPFILTERS ( equipment_health[vibration_alert] = TRUE () )
)

アラート設備数 =
CALCULATE (
    DISTINCTCOUNT ( equipment_health[equipment_id] ),
    KEEPFILTERS (
        FILTER (
            ALL ( equipment_health[temperature_alert], equipment_health[vibration_alert] ),
            equipment_health[temperature_alert] || equipment_health[vibration_alert]
        )
    )
)

異常発生設備数 =
CALCULATE (
    DISTINCTCOUNT ( equipment_health[equipment_id] ),
    KEEPFILTERS ( equipment_health[abnormal_count] > 0 )
)

平均温度 (7日) =
AVERAGE ( equipment_health[rolling_7d_avg_temperature] )

最高温度 =
MAX ( equipment_health[daily_max_temperature] )

平均振動 (7日) =
AVERAGE ( equipment_health[rolling_7d_avg_vibration] )

最大振動 =
MAX ( equipment_health[daily_max_vibration] )
```

- `平均温度 (7日)` / `平均振動 (7日)` は**設備 × 日**の粒度（1 行）で使うと正確な値になる。ライン別・全体で表示すると設備の単純平均になるため、ビジュアルのタイトルに「設備平均」と明記する。温度・振動は設備ごとに独立した物理量であり、件数で重み付けする意味が薄いため単純平均とする
- アラートの判定（> 65℃ / > 3.5 mm/s）は ETL 側で済んでいるフラグを使い、DAX で再判定しない（判定ロジックを 1 か所に保つ）
- 期間が複数日にわたる場合、アラート設備数は「期間中に 1 回以上アラートが出た設備数」になる

### 5.3 `03_設備\最終日`（カード用）

| メジャー | 書式 | 説明 |
|---------|------|------|
| `最終日 (設備)` | `yyyy/mm/dd` | 現在の期間フィルター内で設備データが存在する最後の日。ライン・設備選択の影響を受けない |
| `監視設備数 (最終日)` | `#,0` | 最終日にデータのある設備数 |
| `アラート設備数 (最終日)` | `#,0` | 最終日にアラートが出ている設備数（0 件は 0 と表示） |
| `異常発生回数 (最終日)` | `#,0` | 最終日の異常/停止回数（0 件は 0 と表示） |
| `正常稼働率 (最終日)` | `0.0%` | 最終日の正常稼働率（簡易） |

```dax
最終日 (設備) =
CALCULATE (
    MAX ( equipment_health[production_date] ),
    REMOVEFILTERS ( dim_line ),
    REMOVEFILTERS ( dim_equipment )
)

監視設備数 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [監視設備数], dim_date[calendar_date] = _d )

アラート設備数 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    COALESCE ( CALCULATE ( [アラート設備数], dim_date[calendar_date] = _d ), 0 )

異常発生回数 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    COALESCE ( CALCULATE ( [異常発生回数], dim_date[calendar_date] = _d ), 0 )

正常稼働率 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [正常稼働率 (簡易)], dim_date[calendar_date] = _d )
```

`COALESCE ( …, 0 )` はカードで「(空白)」ではなく「0」と表示するために使う。マトリックスやテーブルで使うと全設備の行が 0 で表示されるため、`アラート設備数 (最終日)` / `異常発生回数 (最終日)` はカード専用とする。

### 5.4 `03_設備\最終日`（設備一覧テーブル用）

設備 1 台 = 1 行のテーブルで最終日の状態を並べるためのメジャー。`COALESCE` を使わないため、最終日にデータのない設備は行ごと表示されない。

| メジャー | 書式 | 説明 |
|---------|------|------|
| `状態 (最終日)` | — | 最終日の `status_summary`（設備 1 台に絞られているときだけ値を返す） |
| `平均温度 (7日) (最終日)` | `0.0" ℃"` | 最終日時点の 7 日移動平均温度 |
| `平均振動 (7日) (最終日)` | `0.00" mm/s"` | 最終日時点の 7 日移動平均振動 |
| `異常回数 (最終日・行)` | `#,0` | 最終日の異常/停止回数（0 は 0 のまま表示） |

```dax
状態 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE (
        SELECTEDVALUE ( equipment_health[status_summary] ),
        dim_date[calendar_date] = _d
    )

平均温度 (7日) (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [平均温度 (7日)], dim_date[calendar_date] = _d )

平均振動 (7日) (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [平均振動 (7日)], dim_date[calendar_date] = _d )

異常回数 (最終日・行) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [異常発生回数], dim_date[calendar_date] = _d )
```

`異常発生回数` は `SUM` のため、データがあれば 0 も 0 として返る（空白になるのはその設備の行が存在しない場合だけ）。

---

## 6. 明細ファクトのメジャー（任意）

`semantic-model.md` 7 章で `production_facts` を追加した場合のみ作成する。ホームテーブルは `production_facts`、表示フォルダーは `04_明細分析`。

| メジャー | 書式 | 説明 |
|---------|------|------|
| `実績数 (明細)` | `#,0` | 明細の実績数量合計 |
| `不良数 (明細)` | `#,0` | 明細の不良数量合計 |
| `不良率 (明細)` | `0.00%` | 品目・作業者・シフト別に分析できる不良率 |
| `作業者数 (明細)` | `#,0` | 作業者のユニーク数 |

```dax
実績数 (明細) =
SUM ( production_facts[actual_quantity] )

不良数 (明細) =
SUM ( production_facts[defect_quantity] )

不良率 (明細) =
DIVIDE ( [不良数 (明細)], [実績数 (明細)] )

作業者数 (明細) =
DISTINCTCOUNT ( production_facts[operator_id] )
```

---

## 7. 目標・閾値と書式

### 7.1 `09_目標・閾値`

| メジャー | 値 | 書式 | 根拠 |
|---------|-----|------|------|
| `達成率 目標` | 1.00 | `0%` | 計画どおり（100%）。デモ用の仮置き |
| `達成率 注意閾値` | 0.95 | `0%` | 計画の 95% 未満で赤。デモ用の仮置き |
| `不良率 目標` | 0.05 | `0.0%` | 要件 `success_criteria` の不良率閾値 5%、`monitoring-design.md` 3.1 の Warning > 5% |
| `不良率 注意閾値` | 0.04 | `0.0%` | 目標の 80%。早期注意用のデモ用仮置き（要件に定義なし） |
| `温度 注意閾値` | 60 | `0" ℃"` | `schema-design.md` 4.2 アラート閾値 Warning（7 日平均 > 60℃） |
| `温度 警報閾値` | 65 | `0" ℃"` | 同 Critical（> 65℃）。`temperature_alert` の判定値と同じ |
| `振動 注意閾値` | 3.0 | `0.0" mm/s"` | 同 Warning（7 日平均 > 3.0 mm/s） |
| `振動 警報閾値` | 3.5 | `0.0" mm/s"` | 同 Critical（> 3.5 mm/s）。`vibration_alert` の判定値と同じ |

```dax
達成率 目標 = 1.00

達成率 注意閾値 = 0.95

不良率 目標 = 0.05

不良率 注意閾値 = 0.04

温度 注意閾値 = 60

温度 警報閾値 = 65

振動 注意閾値 = 3.0

振動 警報閾値 = 3.5
```

### 7.2 `09_書式`

条件付き書式の「書式スタイル: フィールド値」に指定する。色は 3 段階（良好 / 注意 / 異常）に統一する。

| メジャー | 判定 | 使う場所 |
|---------|------|---------|
| `_色 良好` / `_色 注意` / `_色 異常` | 色定数 | 他の色メジャーから参照する（ここだけに HEX を書く） |
| `達成率 色` | ≥ 目標: 良好、≥ 注意閾値: 注意、それ未満: 異常 | ライン別マトリックスの背景色 |
| `達成率 色 (最終日)` | 上記を最終日で評価 | ライン別（最終日）マトリックス、カードの値の色 |
| `不良率 色` | > 目標: 異常、> 注意閾値: 注意、それ以下: 良好 | 不良率ヒートマップ・棒グラフ |
| `不良率 色 (最終日)` | 上記を最終日で評価 | 不良率カードの値の色 |
| `温度 色` | > 警報閾値: 異常、> 注意閾値: 注意、それ以下: 良好 | 温度ヒートマップ |
| `振動 色` | 同上（振動の閾値） | 振動ヒートマップ |
| `設備状態 色` | アラートまたは異常/停止あり: 異常、温度・振動が注意閾値超: 注意、それ以外: 良好 | 設備別の期間集計 |
| `温度 色 (最終日)` / `振動 色 (最終日)` / `設備状態 色 (最終日)` | 上記 3 つを設備の最終日で評価 | 設備一覧テーブル（最終日） |

```dax
_色 良好 = "#1E8E3E"

_色 注意 = "#F9A825"

_色 異常 = "#D93025"

達成率 色 =
VAR _v = [達成率]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( _v ), BLANK (),
        _v >= [達成率 目標], [_色 良好],
        _v >= [達成率 注意閾値], [_色 注意],
        [_色 異常]
    )

達成率 色 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [達成率 色], dim_date[calendar_date] = _d )

不良率 色 =
VAR _v = [不良率]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( _v ), BLANK (),
        _v > [不良率 目標], [_色 異常],
        _v > [不良率 注意閾値], [_色 注意],
        [_色 良好]
    )

不良率 色 (最終日) =
VAR _d = [最終日]
RETURN
    CALCULATE ( [不良率 色], dim_date[calendar_date] = _d )

温度 色 =
VAR _v = [平均温度 (7日)]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( _v ), BLANK (),
        _v > [温度 警報閾値], [_色 異常],
        _v > [温度 注意閾値], [_色 注意],
        [_色 良好]
    )

振動 色 =
VAR _v = [平均振動 (7日)]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( _v ), BLANK (),
        _v > [振動 警報閾値], [_色 異常],
        _v > [振動 注意閾値], [_色 注意],
        [_色 良好]
    )

設備状態 色 =
VAR _measured = [計測数]
VAR _alerts = [アラート設備数]
VAR _abnormal = [異常発生回数]
VAR _temp = [平均温度 (7日)]
VAR _vib = [平均振動 (7日)]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( _measured ), BLANK (),
        _alerts > 0 || _abnormal > 0, [_色 異常],
        _temp > [温度 注意閾値] || _vib > [振動 注意閾値], [_色 注意],
        [_色 良好]
    )

温度 色 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [温度 色], dim_date[calendar_date] = _d )

振動 色 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [振動 色], dim_date[calendar_date] = _d )

設備状態 色 (最終日) =
VAR _d = [最終日 (設備)]
RETURN
    CALCULATE ( [設備状態 色], dim_date[calendar_date] = _d )
```

`温度 色` / `振動 色` / `設備状態 色` のホームテーブルは `equipment_health` でもよい（表示フォルダーは `09_書式` に揃える）。

---

## 8. 検証クエリ

メジャー作成後、DAX クエリ ビューで次を実行して値を確認する。`dax-measures.dax` の末尾にも同じクエリを含めている。

```dax
DEFINE
    VAR __last_equipment_date = [最終日 (設備)]

// 比率の再計算と行平均の違いを確認する（デモの説明にも使える）
EVALUATE
ROW (
    "達成率 (合計から再計算)", [達成率],
    "達成率 (行平均・誤り)", AVERAGE ( daily_production_kpi[achievement_rate] ),
    "不良率 (合計から再計算)", [不良率],
    "不良率 (行平均・誤り)", AVERAGE ( daily_production_kpi[defect_rate] )
)

// ライン別の最終日 KPI
EVALUATE
SUMMARIZECOLUMNS (
    dim_line[line_name],
    "最終日", [最終日],
    "実績数", [実績数 (最終日)],
    "達成率", [達成率 (最終日)],
    "前日比", [実績数 前日比 (最終日)],
    "不良率", [不良率 (最終日)]
)
ORDER BY dim_line[line_name]

// 設備別の最終日の状態
EVALUATE
SUMMARIZECOLUMNS (
    dim_equipment[equipment_name],
    TREATAS ( { __last_equipment_date }, dim_date[calendar_date] ),
    "平均温度 (7日)", [平均温度 (7日)],
    "平均振動 (7日)", [平均振動 (7日)],
    "異常発生回数", [異常発生回数],
    "アラート設備数", [アラート設備数]
)
ORDER BY dim_equipment[equipment_name]
```

| 確認項目 | 期待値 |
|---------|--------|
| `達成率 (合計から再計算)` と Gold のライン × 日の `achievement_rate` | 1 ライン × 1 日に絞ったときだけ一致する（丸め誤差 0.0001 以内） |
| `実績数 (最終日)` の合計 | Lakehouse で `SELECT SUM(total_actual_quantity) FROM gold.daily_production_kpi WHERE production_date = <最終日>` と一致する |
| `アラート設備数` | `equipment_health` の最終日で `temperature_alert OR vibration_alert` の設備数と一致する |
