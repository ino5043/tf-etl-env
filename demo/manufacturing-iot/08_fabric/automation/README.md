# Fabric 自動配置スクリプト

`../deploy-guide.md` の手順 2〜9（ワークスペース作成から Power BI レポートの画面キャプチャまで）を、Fabric / Power BI の REST API で自動化する。GUI 操作が必要なのは、Fabric 試用版の開始（手順 1）だけである。

処理本体は Node.js（`deploy.js` と `lib/`）である。`deploy.sh` は `node deploy.js` を呼ぶだけの互換用ラッパーで、従来のコマンドはそのまま使える。

## 前提

| 項目 | 内容 |
|------|------|
| Fabric | 試用版を開始済み（または F SKU の容量がある）。容量 ID を `config.sh` に設定する |
| Azure CLI | `az login` で、Fabric を使う組織アカウントにサインイン済み。アクセストークンは `az account get-access-token` で取得する |
| Node.js | 22.15 以上（24 推奨）。組み込みの `fetch` と `--use-system-ca` を使う。配置そのものに npm パッケージは不要 |
| 画面キャプチャ（`shots`） | `npm install`（`puppeteer-core` と `powerbi-client` を入れる）と Chromium 系ブラウザー（既定は Edge。`BROWSER_PATH` で変更） |
| bash（任意） | `./deploy.sh` を使う場合のみ。Git Bash（Windows）または Linux / macOS の bash |

## 使い方

```bash
cd demo/manufacturing-iot/08_fabric/automation
cp config.example.sh config.sh     # CAPACITY_ID などを設定する
./deploy.sh all                    # すべて実行（30 日分の処理を含め 25〜35 分程度）
./deploy.sh model report shots     # 手順を選んで実行

# bash を使わない場合（PowerShell / cmd でも同じ）
node --use-system-ca deploy.js all
npm run deploy -- model report
```

作成した項目の ID は `$WORK_DIR/state.json` に保存され、後続の手順で使われる（bash 版の `state.env` があれば初回に読み込んで引き継ぐ）。`state.json` にはワークスペース名も記録し、`WORKSPACE_NAME` と食い違う場合は何もせずに止める（`STATE_MISMATCH`。別の `WORK_DIR` を使うか `state.json` を削除する）。名前が未記録の古い状態では警告（`state_workspace_unverified`）を出して続行し、`workspace` 手順で記録する。どの手順も同じ表示名の項目があれば作り直さずに定義を更新するため、何度実行してもよい。

| 終了コード | 意味 |
|-----------|------|
| 0 | 指定した手順がすべて成功 |
| 1 | いずれかの手順が失敗（設定エラー・API エラー・検証の不一致・メジャーのエラー・キャプチャ失敗を含む）。失敗した手順で止まり、以降は実行しない |
| 2 | 手順名の誤り（何も実行しない） |

## 手順

| 手順 | 内容 | 対応する手動手順 | 目安 |
|------|------|----------------|------|
| `workspace` | ワークスペースを作成し、容量に割り当てる | 手順 2 | 数秒 |
| `lakehouse` | Lakehouse をスキーマ有効で作成する | 手順 3 | 数秒 |
| `upload` | `../sample_data/*.csv` を `Files/sample_data/` にアップロードする | 手順 4 | 1 分 |
| `notebook` | `../notebook/fabric_pipeline.ipynb` を取り込み、既定の Lakehouse に紐づける | 手順 5 | 数秒 |
| `run` | Notebook を実行する（`TARGET_DATE` 空 = 30 日分をバックフィル） | 手順 5 | 15 分前後 |
| `verify` | 検証クエリ（手順 6 の検証 1〜5）を Notebook で実行し、期待値と照合する。不一致があれば失敗する | 手順 6 | 1〜2 分 |
| `model` | テーブル定義を取得し、Direct Lake のセマンティックモデル（TMDL）を作成・更新して更新（リフレッシュ）し、全メジャーを評価する。エラーのメジャーが 1 つでもあれば失敗する | 手順 8 | 2〜3 分 |
| `report` | `../powerbi/dashboard-spec.md` の 3 ページのレポート（PBIR）を作成・更新する | 手順 8 | 数秒 |
| `pipeline` | Data Pipeline（ノートブック アクティビティ）を作成・更新し、`TARGET_DATE` の日で 1 回実行する | 手順 7 | 3 分前後 |
| `shots` | レポートの 3 ページを埋め込み表示して PNG に保存する（`$WORK_DIR/shots/`）。描画できないページがあれば失敗する | 手順 9 の一部 | 1 分 |

スケジュール実行（毎日 4:00）は設定しない。試用版容量を消費するため、必要な場合はパイプラインの画面から設定する。

## 設定

| 項目 | 既定値 | 説明 |
|------|--------|------|
| `CAPACITY_ID` | （必須） | ワークスペースを割り当てる容量の GUID |
| `WORKSPACE_NAME` / `LAKEHOUSE_NAME` / `MODEL_NAME` / `REPORT_NAME` / `PIPELINE_NAME` | `config.example.sh` のとおり | 作成する項目の表示名。`LAKEHOUSE_NAME` は英字で始まる英数字と `_` のみ |
| `TARGET_DATE` / `DEMO_FROM` / `DEMO_TO` | `2026-10-30` / `2026-10-01` / `2026-10-31` | `YYYY-MM-DD`。実在しない日付や `DEMO_FROM >= DEMO_TO` はエラー |
| `WORK_DIR` | `automation/.work` | 作業フォルダー（状態ファイル、API 本文、TMDL / PBIR、検証結果、画面キャプチャ） |
| `REQUESTS_CA_BUNDLE` | なし | Azure CLI に渡す CA バンドル（下記「はまりどころ」） |
| `CONFIG`（環境変数のみ） | `automation/config.sh` | 設定ファイルの場所 |
| `SHOTS_TIMEOUT_MS`（環境変数のみ） | `300000`（5 分） | `shots` の子プロセス全体の上限。超えたら終了させ、手順を失敗させる |

- `config.sh` は**シェルとして実行しない**。`KEY="value"` / `KEY='value'` / `KEY=value`（先頭の `export` 可）の行とコメントだけを読む。`$VAR` や `$(...)` などの展開は書けない（書くと設定エラーになる）。bash 版と異なり、`config.sh` に書いたコマンドが実行されることはない
- `"…"` の中の `\` は bash と同じく `\$` `` \` `` `\"` `\\` だけがエスケープで、それ以外はそのまま残る。Windows のパスは `WORK_DIR="C:\work\fabric-demo"` のように書ける（`'…'` でも可。引用符なしで `\` は書けない）
- 同じ名前の環境変数があれば `config.sh` より優先する（例: `WORKSPACE_NAME=demo2 ./deploy.sh workspace`）
- 起動時にすべての項目を検証し、問題をまとめて表示してから止まる。API は 1 つも呼ばない

### 作業フォルダー（WORK_DIR）

`.work/` には ID を含む API 本文や画面キャプチャ（ワークスペース名・アカウント名が映る）が保存される。`.gitignore` 済みだが、リポジトリが OneDrive などの同期フォルダーにある場合はクラウドにも同期される。同期されない場所を指定することを推奨する。

```bash
WORK_DIR="C:/work/fabric-demo" ./deploy.sh all      # 環境変数で指定
# または config.sh に WORK_DIR="C:/work/fabric-demo" と書く（相対パスは実行時のカレントフォルダー基準）
```

## ログ

進捗とエラーは標準エラーに JSON Lines（1 行 1 オブジェクト）で出る。アクセストークンは出力しない。

```json
{"ts":"2026-10-30T01:02:03.456Z","level":"info","event":"step_completed","step":"model","status":"ok","duration_ms":143210,"model_id":"…","created":false,"measures":80}
{"ts":"2026-10-30T01:05:00.000Z","level":"error","event":"step_failed","step":"report","status":"failed","duration_ms":812,"error_type":"HttpError","code":"HTTP_STATUS","message":"POST https://api.fabric.microsoft.com/v1/… -> HTTP 400: …","http_status":400,"body_excerpt":"{\"errorCode\":\"…\"}"}
```

| event | 内容 |
|-------|------|
| `deploy_started` / `deploy_completed` / `deploy_failed` | 全体の開始・完了（保存した ID）・失敗（失敗した手順） |
| `step_started` / `step_completed` / `step_failed` | 手順ごとの開始・完了・失敗。`duration_ms` と作成・更新した ID を含む |
| `job_status` / `model_refresh_status` | Notebook・パイプラインのジョブ、モデル更新の状態（ポーリングごと） |
| `http_retry` | 一時的なエラー（通信エラー、5xx、429）での再試行。待ち時間 `delay_ms` |
| `sql_endpoint_refresh_failed` | SQL 分析エンドポイントの同期 API の失敗（警告のみで続行） |

`jq` で読む例: `./deploy.sh all 2> deploy.log; jq -c 'select(.level != "info")' deploy.log`

## 信頼性と安全性

| 項目 | 内容 |
|------|------|
| タイムアウト | すべての HTTP 要求に 60 秒のタイムアウト。長時間処理（202 応答）は 15 分、Notebook・パイプラインのジョブは 30〜60 分、モデル更新は 15 分を上限に待つ |
| 再試行 | 429 は `Retry-After` に従って再試行する（POST も含む）。通信エラー・5xx は、再送しても結果が変わらない要求（GET、OneLake への書き込み、定義更新 `updateDefinition`、SQL エンドポイントの同期、DAX クエリ）だけを指数バックオフで最大 4 回再試行する。項目作成・ジョブ開始などの POST は二重実行を避けるため再試行しない（手順を再実行すれば表示名で既存の項目を見つけて更新する） |
| ステータス確認 | 作成・定義更新・更新（リフレッシュ）・アップロードの応答ステータスをすべて確認し、想定外なら本文の抜粋付きで失敗させる |
| モデル更新の待機 | 更新要求の応答の要求 ID（`Location` の `/refreshes/{id}` を優先し、なければ `RequestId`、最後に `x-ms-request-id`）と更新履歴の `requestId` を照合し、前回の更新の結果を取り違えない |
| トークン | Azure CLI の標準出力から受け取り、対象（audience）ごとに期限の 5 分前までキャッシュする。コマンドライン引数には載せない。`az` が失敗したらその場で止める。`az` は PATH から絶対パスで解決し（カレントフォルダーは探さない）、`NoDefaultCurrentDirectoryInExePath=1` を付けて起動する |
| 送信先の制限 | トークンを付けて送るのは `https://api.fabric.microsoft.com`・`https://onelake.dfs.fabric.microsoft.com`・`https://api.powerbi.com` と、長時間処理の `Location` に返る Fabric の地域ホスト（`wabi-…-redirect.analysis.windows.net`）だけ。202 応答の `Location` もこれら以外なら追わない。HTTP のリダイレクト（3xx）は追わずに失敗させる |
| ログ | トークン類のキー（`token` / `authorization` など）は入れ子の中も含めて出力しない。`Bearer …` や JWT に見える値は `[REDACTED]` に置き換える |
| 画面キャプチャの上限 | `shots` の子プロセスは `SHOTS_TIMEOUT_MS`（既定 5 分）で終了させる（ログ `shots_timeout`、終了コード 124） |
| 画面キャプチャ | `powerbi-client` は `node_modules` のファイルを注入する（CDN から読み込まない）。ブラウザーのサンドボックスは有効のまま（Linux を root で動かす場合だけ `--no-sandbox`） |

## ファイル構成

| ファイル | 役割 |
|---------|------|
| `deploy.js` | エントリーポイント（引数の解釈、設定の読み込み、各手順の実行） |
| `deploy.sh` | 互換用ラッパー（`node --use-system-ca deploy.js "$@"`） |
| `config.example.sh` | 設定の見本（`config.sh` にコピーして使う。`config.sh` は Git の管理対象外） |
| `package.json` | テストのコマンドと、画面キャプチャ用の依存パッケージ（バージョン固定） |
| `lib/steps.js` | 各手順の処理 |
| `lib/runner.js` | 手順名の解決と順次実行、手順ごとの構造化ログ |
| `lib/config.js` | `config.sh` の解釈（実行しない）と検証 |
| `lib/state.js` | 状態ファイル（`state.json`）の読み書き。更新は新しいオブジェクトを返す |
| `lib/http.js` | `fetch` のラッパー（タイムアウト、再試行、`Retry-After`、型付きエラー） |
| `lib/auth.js` | Azure CLI によるトークン取得とキャッシュ |
| `lib/api.js` / `lib/endpoints.js` | 送信先ホストの検証と、ホストに応じたトークンの付与 |
| `lib/fabric.js` | Fabric API（一覧のページング、作成または定義更新、長時間処理の待機、ジョブ実行） |
| `lib/onelake.js` | OneLake へのアップロード・ダウンロード |
| `lib/powerbi.js` | Power BI API（モデル更新の待機、DAX クエリ） |
| `lib/log.js` / `lib/errors.js` | JSON Lines のログ、型付きエラー |
| `scripts/make_notebook_body.js` | Notebook 作成 API の本文を作る（`.py` は 1 セルの Notebook に変換） |
| `scripts/build_model.js` | `../powerbi/dax-measures.dax` / `dax-measures.md` と実テーブル定義から TMDL を生成する |
| `scripts/build_report.js` | レポート定義（PBIR）を生成する |
| `scripts/build_pipeline.js` | Data Pipeline の定義を生成する |
| `scripts/check_all_measures.js` | 全メジャーを 1 つずつ DAX で評価し、エラーを列挙する（エラーがあれば終了コード 1） |
| `scripts/check_verify.js` | 検証結果を手順 6 の期待値と照合する（不一致があれば終了コード 1） |
| `scripts/shoot_report.js` | レポートを Power BI の JavaScript ライブラリで埋め込み表示し、PNG を撮る（描画できなければ終了コード 1） |
| `notebooks/verify_pipeline.py` | 検証クエリ（Spark SQL）を実行し、結果を `Files/_verify/` に保存する |
| `notebooks/schema_dump.py` | Gold テーブルの列定義を `Files/_verify/` に保存する（TMDL の生成に使う） |
| `test/` | `node:test` のテスト（HTTP とトークン取得はすべて偽物に差し替える。実際の API は呼ばない） |

`scripts/*.js` は単独のコマンドとしても従来どおり使える（引数は各ファイル先頭の Usage を参照）。`deploy.js` からは関数として呼ぶ。

## テスト

```bash
npm test     # 依存パッケージのインストールは不要
```

| 出力 | 内容 |
|------|------|
| 標準出力 | テスト結果とカバレッジの表 |
| `test-results/junit.xml` | JUnit XML |
| `test-results/coverage.txt` | テスト結果とカバレッジの表（テキスト） |
| `test-results/lcov.info` | カバレッジ（lcov。`genhtml` などで HTML にできる） |

行カバレッジが 80% 未満の場合はテストが失敗する。`test-results/` は Git の管理対象外である。

## はまりどころ

| 症状 | 原因と対処 |
|------|-----------|
| Azure CLI が `CERTIFICATE_VERIFY_FAILED` になる | ウイルス対策ソフト（SSL/TLS スキャン）やプロキシが HTTPS を検査している。その検査用ルート証明書を Azure CLI の CA バンドル（certifi）に追記したファイルを作り、`REQUESTS_CA_BUNDLE` に設定する。検証自体は無効にしない |
| Node.js の通信が `UNABLE_TO_GET_ISSUER_CERT_LOCALLY` / `SELF_SIGNED_CERT_IN_CHAIN` で失敗する（ログの `error_type` が `NetworkError`） | 同じく HTTPS 検査が原因。`node --use-system-ca`（`deploy.sh` と `npm run deploy` は既定で付ける）で OS の証明書ストアを信頼させる。OS のストアに検査用ルート証明書がない場合は、その証明書の PEM を `NODE_EXTRA_CA_CERTS` に設定する |
| `AuthError: token for … failed` | `az login` していない、またはサインインの期限切れ。`az login` をやり直す。`az` が見つからない場合は Azure CLI をインストールする |
| `invalid config` で止まる | 表示された項目を `config.sh` で直す。`$(...)` などシェルの書き方は使えない |
| `model` が `refresh Failed`（「Delta テーブルにアクセスできない」など） | Notebook 直後は SQL 分析エンドポイントのテーブル同期が遅れることがある。`model` は同期 API を呼んでから更新するが、それでも失敗する場合は数分おいて `model` を再実行する |
| `model` が `MEASURE_ERRORS` で止まる | メッセージに失敗したメジャーと DAX のエラーが出る。`../powerbi/dax-measures.dax` を直して `model` を再実行する |
| `dax parse mismatch` | `dax-measures.dax` の `MEASURE` 行の数と解析できたメジャーの数が合わない。テーブル名・メジャー名の書き方（`'名前'[メジャー]`）とインデントを確認する |
| `empty expression for measure …` | メジャーの式が空になった。式の本体は `MEASURE` 行より深くインデントする（同じ深さに書くと別の行とみなされる） |
| `URL not allowed` | 長時間処理の `Location` が想定外のホストを指している。トークンの送信を避けるため止めている。ホストが正当なものなら `lib/endpoints.js` の `OPERATION_HOSTS` を見直す |
| `shots` で画像が撮れない | `npm install` 済みか、ブラウザーの場所（`BROWSER_PATH`）を確認する。テナント設定で「レポートの画像エクスポート」が無効でも動くよう、ExportTo API ではなく埋め込み表示で撮っている。「アプリへのコンテンツの埋め込み」が無効の場合は使えない |

## 注意

- 作成されるのは試用版容量上の項目だけである。不要になったらワークスペースごと削除する
- 画面キャプチャにはワークスペース名・アカウント名が映り込む。公開資料に使う場合はマスクする
