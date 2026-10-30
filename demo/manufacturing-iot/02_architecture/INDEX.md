# ADR INDEX — 製造業 IoT デモ

系列接頭辞: **MFG**（他の系列から参照するときは `MFG-NNN` と書く。tf-standard-env の rules/adr.md「7. 系列と参照表記」）

| # | タイトル | ステータス | 作成日 | 作成者 | 影響範囲 | 備考 |
|---|---------|-----------|--------|--------|---------|------|
| 001 | データ取り込み方式の選択 | 承認済 | 2026-03-15 | Claude AI（pipeline-architect agent） | 全パイプライン | `ADR-001_pipeline-pattern.md` |
| 002 | Power BI セマンティックモデルを Direct Lake で構成する | 承認済 | 2026-10-30 | 猪野 勉 / Claude AI | Gold、Power BI | ディメンションは Lakehouse 側で作成 |
| 003 | Fabric への配置を REST API と生成した定義ファイルで自動化する | 承認済 | 2026-10-30 | 猪野 勉 / Claude AI | `08_fabric/automation/`、`powerbi/` | 画面は埋め込み表示で撮影 |
