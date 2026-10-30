# 08_fabric/design — Microsoft Fabric 移行・セキュリティ設計

オンプレミスの工場システムを持つ製造業が、データ基盤を Microsoft Fabric へ移行する際の参照設計である。本デモ（工場生産実績 ETL）を具体例として使い、顧客ごとの現状分析結果に合わせて調整して使う。

| ドキュメント | 概要 |
|-------------|------|
| [migration-onprem-to-fabric.md](migration-onprem-to-fabric.md) | オンプレ → Fabric 移行設計 |
| [security-entra-id.md](security-entra-id.md) | Entra ID 連携のセキュリティ設計 |
| [diagrams/](diagrams/) | Mermaid ソース（`.mmd`）と SVG |

## migration-onprem-to-fabric.md

MES・生産管理 DB・ファイルサーバー・イントラサービスを持つ工場を対象に、現状分析チェックリスト、ターゲットアーキテクチャ（On-premises data gateway → Data Factory パイプライン / Dataflow Gen2 / Mirroring / Shortcuts / Eventstream → OneLake Medallion → Direct Lake / Power BI）、取り込み方式の比較と選定基準を示す。移行は Phase 0（PoC）→ 1（並行稼働）→ 2（切替）→ 3（旧系廃止・最適化）で進め、各フェーズの入口・出口基準、旧系との日次突合（件数・数量合計・キー集合・行ハッシュ）、ロールバック計画を定義する。あわせて、イントラサービスを Fabric と Azure サーバーレス（Functions / Container Apps / Logic Apps 等）へ移す判断フローと手順、容量（F SKU）のサイジングとワークスペース分離・ゲートウェイクラスタ等による負荷分散、リスク表、中規模移行の WBS 例を収録する。

## security-entra-id.md

Entra ID グループを単位とした ID モデル（管理者・エンジニア・分析担当・工場長・監査）、ワークスペースロール・項目権限・Lakehouse / SQL 分析エンドポイント・セマンティックモデル RLS を横断する権限マトリクスを定義する。工場長が自工場のラインのみ閲覧できる RLS を、セマンティックモデルの静的ロール・動的ロール（DAX）と SQL のセキュリティポリシーの 3 方式で示し、アクセス経路ごとに効く範囲の違いを整理する。さらに、workspace identity・サービスプリンシパル・Key Vault による実行 ID とシークレット管理、ゲートウェイ配置・Private Link・マネージドプライベートエンドポイント・条件付きアクセス・テナント設定によるネットワークと共有の制御、秘密度ラベルと PII 保護（`rules/etl-security.md` 準拠）、監査ログと監視、設計時チェックリストを収録する。

## 図の再生成

```bash
cd demo/manufacturing-iot/08_fabric/design/diagrams
for f in *.mmd; do npx mmdc -i "$f" -o "${f%.mmd}.svg" -t neutral -b transparent; done
```

mmdc 同梱の Chromium が起動しない環境（Windows など）では、インストール済みのブラウザを指定する。

```bash
echo '{"executablePath":"C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe","args":["--no-sandbox"]}' > puppeteer.json
for f in *.mmd; do npx mmdc -p puppeteer.json -i "$f" -o "${f%.mmd}.svg" -t neutral -b transparent; done
rm puppeteer.json
```

> 本書の Fabric 機能の記述は作成時点の情報に基づく。「プレビュー機能の可能性あり」と記した機能は、採用前に Microsoft Learn と自テナントで提供状況を確認すること。
