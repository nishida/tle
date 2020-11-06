# TLE

space-track.org から軌道要素データを取得する。

## 環境
- Python 3.7以降
- pandas, spacetrack, sgp4, xmljson, jupyter

## 使い方

#### space-track.org のアカウント設定
`spacetrackaccount.py.sample` を `spacetrackaccount.py` にコピーし、IDとパスワードを記述する。

#### Satellite Catalog Number を指定して取得

tle API を用いて衛星番号30000の過去の軌道要素データをJSON形式で取得する:

    $ ./download_tle_satcat_json.py

衛星番号30000から30029までの軌道要素データを順番に取得する:

    $ ./download_tle_satcat_json.py 30000 30029

1リクエストで10個の衛星の軌道要素データを取得することで、リクエスト回数を減らす:

    $ ./download_tle_satcat_json.py 30000 30029 10

ただし、リクエスト1回あたりのデータ量が多すぎるとエラーとなる。

#### 日付を指定して取得

gp_history API を用いて2019年1月12日から18日までの全ての軌道要素データをJSON形式で取得する:

    $ ./download_gp_date_json.py 2019/1/12 2019/1/18

tle API を使った場合と項目名が異なることに注意。

#### CSVに変換

JSONファイルをCSVファイルに変換する。JSONファイルは圧縮されていても可。出力されるファイルの拡張子は `.csv` となる。

    $ ./json2csv.py 2020-10-10.json.xz

#### TLEを取り出す

JSONファイルから、TLEを取り出す。JSONファイルは圧縮されていても可。出力されるファイルの拡張子は `.tle` となる。

    $ ./json2tle.py 2020-10-10.json.xz

#### Jupyter Notebookでいろいろテスト

- `spacetracktest1.ipynb` tle, tle_latest APIを用いたダウンロードのテスト
- `spacetracktest1-gp.ipynb1` gp, gp_history APIを用いたダウンロードのテスト
- `spacetracktest2.ipynb` ダウンロードしたデータをプロット
- `spacetracktest3.ipynb` ダウンロードしたデータをプロット



