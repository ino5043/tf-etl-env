# Fabric 自動配置スクリプト

`../deploy-guide.md` の手順 2〜9（ワークスペース作成から Power BI レポートの画面キャプチャまで）を、Fabric / Power BI の REST API で自動化する。GUI 操作が必要なのは、Fabric 試用版の開始（手順 1）だけである。

## 前提

| 項目 | 内容 |
|------|------|
| Fabric | 試用版を開始済み（または F SKU の容量がある）。容量 ID を `config.sh` に設定する |
| Azure CLI | `az login` で、Fabric を使う組織アカウントにサインイン済み |
| Node.js | 18 以上（`fetch` を使う） |
| bash | Git Bash（Windows）または Linux / macOS の bash |
| 画面キャプチャ（`shots`） | `puppeteer-core` と Chromium 系ブラウザー（既定は Windows の Edge。`BROWSER_PATH` で変更） |

## 使い方

```bash
cd demo/manufacturing-iot/08_fabric/automation
cp config.example.sh config.sh     # CAPACITY_ID などを設定する
./deploy.sh all                    # すべて実行（30 日分の処理を含め 25〜35 分程度）
./deploy.sh model report shots     # 手順を選んで実行
```

作成した項目の ID は `.work/state.env` に保存され、後続の手順で使われる。どの手順も同じ名前の項目があれば作り直さずに更新するため、何度実行してもよい。

## 手順

| 手順 | 内容 | 対応する手動手順 | 目安 |
|------|------|----------------|------|
| `workspace` | ワークスペースを作成し、容量に割り当てる | 手順 2 | 数秒 |
| `lakehouse` | Lakehouse をスキーマ有効で作成する | 手順 3 | 数秒 |
| `upload` | `../sample_data/*.csv` を `Files/sample_data/` にアップロードする | 手順 4 | 1 分 |
| `notebook` | `../notebook/fabric_pipeline.ipynb` を取り込み、既定の Lakehouse に紐づける | 手順 5 | 数秒 |
| `run` | Notebook を実行する（`TARGET_DATE` 空 = 30 日分をバックフィル） | 手順 5 | 15 分前後 |
| `verify` | 検証クエリ（手順 6 の検証 1〜5）を Notebook で実行し、期待値と照合する | 手順 6 | 1〜2 分 |
| `model` | テーブル定義を取得し、Direct Lake のセマンティックモデル（TMDL）を作成・更新し、全メジャーを評価する | 手順 8 | 2〜3 分 |
| `report` | `../powerbi/dashboard-spec.md` の 3 ページのレポート（PBIR）を作成・更新する | 手順 8 | 数秒 |
| `pipeline` | Data Pipeline（ノートブック アクティビティ）を作成し、`TARGET_DATE` の日で 1 回実行する | 手順 7 | 3 分前後 |
| `shots` | レポートの 3 ページを埋め込み表示して PNG に保存する（`.work/shots/`） | 手順 9 の一部 | 1 分 |

スケジュール実行（毎日 4:00）は設定しない。試用版容量を消費するため、必要な場合はパイプラインの画面から設定する。

## ファイル構成

| ファイル | 役割 |
|---------|------|
| `deploy.sh` | 各手順の実行 |
| `config.example.sh` | 設定の見本（`config.sh` にコピーして使う。`config.sh` は Git の管理対象外） |
| `lib/fabric.sh` | Fabric / OneLake / Power BI REST API の共通関数（トークン取得、長時間処理の待機、ジョブ実行、アップロード） |
| `scripts/make_notebook_body.js` | Notebook 作成 API の本文を作る（`.py` は 1 セルの Notebook に変換） |
| `scripts/build_model.js` | `../powerbi/dax-measures.dax` / `dax-measures.md` と実テーブル定義から TMDL を生成する |
| `scripts/build_report.js` | レポート定義（PBIR）を生成する |
| `scripts/build_pipeline.js` | Data Pipeline の定義を生成する |
| `scripts/check_all_measures.js` | 全メジャーを 1 つずつ DAX で評価し、エラーを列挙する |
| `scripts/check_verify.js` | 検証結果を手順 6 の期待値と照合する |
| `scripts/shoot_report.js` | レポートを Power BI の JavaScript ライブラリで埋め込み表示し、PNG を撮る |
| `notebooks/verify_pipeline.py` | 検証クエリ（Spark SQL）を実行し、結果を `Files/_verify/` に保存する |
| `notebooks/schema_dump.py` | Gold テーブルの列定義を `Files/_verify/` に保存する（TMDL の生成に使う） |

## はまりどころ

| 症状 | 原因と対処 |
|------|-----------|
| Azure CLI が `CERTIFICATE_VERIFY_FAILED` になる | ウイルス対策ソフト（SSL/TLS スキャン）やプロキシが HTTPS を検査している。その検査用ルート証明書を Azure CLI の CA バンドル（certifi）に追記したファイルを作り、`REQUESTS_CA_BUNDLE` に設定する。検証自体は無効にしない |
| `model` の更新で「Delta テーブルにアクセスできない」 | Notebook 直後は SQL 分析エンドポイントのテーブル同期が遅れることがある。`model` は同期 API を呼んでから更新するが、それでも失敗する場合は数分おいて `model` を再実行する |
| `shots` で画像が撮れない | テナント設定で「レポートの画像エクスポート」が無効でも動くよう、ExportTo API ではなく埋め込み表示で撮っている。「アプリへのコンテンツの埋め込み」が無効の場合は使えない |
| 日本語を含む JSON で `Unable to translate bytes` | コマンドライン引数に日本語の JSON を直接渡すと文字コード変換で壊れる。本文は必ずファイル経由（`--data-binary @file`）で送る |

## 注意

- 作成されるのは試用版容量上の項目だけである。不要になったらワークスペースごと削除する
- 画面キャプチャにはワークスペース名・アカウント名が映り込む。公開資料に使う場合はマスクする
