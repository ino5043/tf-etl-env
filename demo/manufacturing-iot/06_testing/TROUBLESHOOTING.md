# テスト環境トラブルシューティング

Windows 環境で PySpark テストを実行する際に遭遇した問題と解決策を記録する。

## 動作確認済み環境

| コンポーネント | バージョン |
|---------------|-----------|
| Python | 3.12.10 (CPython 公式版) |
| JDK | 17 (Eclipse Temurin) |
| PySpark | 3.5.6 |
| delta-spark | 3.2.1 |
| pandas + pyarrow | 最新 |

## 問題と対策一覧

### 1. Python worker がクラッシュする

**エラー**: `Python worker exited unexpectedly (crashed)` / `WinError 10038`

**原因**: Microsoft Store 版 Python のサンドボックスがソケット通信をブロック

**対策**: CPython 公式版をインストール
```bash
winget install Python.Python.3.12
```

**判別方法**:
```bash
where python
# Store 版: C:\Users\...\WindowsApps\PythonSoftwareFoundation.Python.3.13...
# 公式版:   C:\Users\...\AppData\Local\Programs\Python\Python312\python.exe
```

### 2. ClassNotFoundException: SparkSubmit

**エラー**: `Java gateway process exited before sending its port number`

**原因**: 日本語パス（OneDrive の「ドキュメント」等）が Java クラスパスを壊す

**対策**: venv を ASCII のみのパスに作成
```bash
python -m venv C:\tmp\mfg-iot-venv
```

### 3. createDataFrame(list) が失敗する

**エラー**: `spark.createDataFrame([(1,'hello')], schema)` で worker crash

**原因**: リストからの変換は Python worker プロセスが必要

**対策**: Arrow + Pandas DataFrame 経由で変換（conftest.py で自動設定済み）
```python
import pandas as pd
pdf = pd.DataFrame(rows)
spark.createDataFrame(pdf, schema=schema)  # Arrow経由 → worker不要
```

### 4. JAVA_HOME の不一致

**対策**: conftest.py が自動的に JDK 17 (Temurin) を検出・設定する。
手動で設定する場合:
```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot"
```

## セットアップ手順（ゼロから）

```bash
# 1. Python 3.12 インストール
winget install Python.Python.3.12

# 2. JDK 17 インストール
winget install EclipseAdoptium.Temurin.17.JDK

# 3. venv 作成（ASCII パスに！）
"C:\Users\ino50\AppData\Local\Programs\Python\Python312\python.exe" -m venv C:\tmp\mfg-iot-venv
source /c/tmp/mfg-iot-venv/Scripts/activate

# 4. パッケージインストール
pip install pyspark==3.5.6 delta-spark==3.2.1 pytest pandas pyarrow setuptools

# 5. テスト実行
cd demo/manufacturing-iot
python -m pytest 06_testing/ -v
```
