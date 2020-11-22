# TLE

space-track.org から軌道要素データを取得する。

## 環境
- Python 3.7以降
- SQLite 3.24以降
- pandas, spacetrack
- (Jupyter Notebook の各ファイルを実行するには) sgp4, xmljson, pyarrow, tables

## 使い方

#### space-track.org のアカウント設定
`spacetrackaccount.py.sample` を `spacetrackaccount.py` にコピーし、IDとパスワードを記述する。

#### Satellite Catalog Number を指定して取得

gp_history API を用いて衛星番号30000の過去の軌道要素データをJSON形式で取得する:

    $ ./download_gp_id.py

衛星番号30000から30029までの軌道要素データを順番に取得する:

    $ ./download_gp_id.py 30000 30029

1リクエストで10個の衛星の軌道要素データを取得することで、リクエスト回数を減らす:

    $ ./download_gp_id.py 30000 30029 10

ただし、リクエスト1回あたりのデータ量が多すぎるとエラーとなる。

#### 日付を指定して取得

gp_history API を用いてEPOCHが2019年1月12日から18日までの全ての軌道要素データをJSON形式で取得する:

    $ ./download_gp_date.py 2019/1/12 2019/1/18

上記の例ではクエリの都合上、もし、EPOCH が2019/1/19 00:00:00.000000 のデータが存在した場合、それも含まれることに注意。
`-c` オプションをつけると、EPOCH の代わりに CREATION_DATE を用いる。

#### CSVに変換

JSONファイルをCSVファイルに変換する。JSONファイルは圧縮されていても可。出力されるファイルの拡張子は `.csv` となる。

    $ ./json2csv.py 2020-10-10.json.xz

#### 複数のファイルをまとめてParquetに変換

注: gp, gp_latest APIでダウンロードしたJSONファイルのみに対応

複数のJSONファイルを `pandas.DataFrame` として読み込み、Parquetファイルに変換する。JSONファイルは圧縮されていても可。出力されるファイルは最後に指定する。Parquet ファイルの圧縮方法は `-c` オプションで指定でき、`pandas.DataFrame.to_parquet` がサポートする圧縮方式をサポートする(デフォルトは snappy)。

    $ ./json2parquet.py -c zstd download/*.json.xz out.parquet

#### データを SQLite3 データベースに格納する

注: gp, gp_latest APIでダウンロードしたJSONファイルのみに対応

複数のJSONファイルまたは`json2parquet.py` で作成した複数のParquetファイルを、SQLite3 データベースに格納する。PRIMARY KEY は `GP_ID`。既にテーブルが存在している場合にはUPSERTを行う。JSONファイルは圧縮されていても可。テーブル名は最後に指定する。

    $ ./json2sqlite3.py download/*.json.xz out.sqlite3 elset
    $ ./json2sqlite3.py download/*.parquet out.sqlite3 elset

#### TLEを取り出す

JSONファイルから、TLEを取り出す。JSONファイルは圧縮されていても可。出力されるファイルの拡張子は `.tle` となる。

    $ ./json2tle.py 2020-10-10.json.xz

#### 衛星情報を取得

satcat API を用いて Satellite Catalog (SATCAT) データを JSON 形式で取得する。

    $ ./download_satcat.py -o satcat.json

SQLite3 データベースに格納する。テーブル名は最後に指定する。

    $ ./satcat2sqlite3.py satcat.json.xz out.sqlite3 satcat

#### Jupyter Notebookでいろいろテスト

- `spacetracktest1.ipynb` tle, tle_latest APIを用いたダウンロードのテスト
- `spacetracktest1-gp.ipynb1` gp, gp_history APIを用いたダウンロードのテスト
- `spacetracktest2.ipynb` ダウンロードしたデータを確認・プロット
- `spacetracktest3.ipynb` ダウンロードしたデータを確認・プロット
- `spacetracktest3-gp.ipynb` ダウンロードしたデータを確認・プロット (gp_history API)
- `spacetracktest4.ipynb` 大量の軌道要素ファイルをまとめて取り扱いやすくするテスト
- `dbtest1.ipynb` SQLite3格納のテスト
- `dbtest2.ipynb` SQLite3格納のテスト (多数のデータでテスト)
- `create_daily_data.ipynb` 日々の軌道高度データを作成する
