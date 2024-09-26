"""
変更履歴：
2024-09-26:
    - GitHubに登録する


"""


#年間利回りを計算する
#コードを実行するだけで、ダイアログボックスの表示機能はない
#データを読み込み　div.csv
#データの保存先　div_r.csv
#データの読み込みと保存先を選択できるように修正する
#データ保全のために、読み込むデータと保存するデータを選択できるように修正したい
#税後利回りも計算するように修正
#利回りの数値は端数処理をしないように修正

import pandas as pd
import numpy as np

# 入力ファイルと出力ファイルの定義
input_file = 'div.csv'
output_file = 'div_r.csv'

# CSVファイルの読み込み
df = pd.read_csv(input_file)

print("元のデータ型:")
print(df.dtypes)
print("\n最初の数行:")
print(df.head())

# 通貨記号の置換辞書と変換対象の列
currency_symbols = {'\$': '', '¥': ''}
columns_to_convert = ['配当/株', '前月終値']  # 必要に応じて他の列も追加

# 通貨記号を削除して数値に変換
for column in columns_to_convert:
    df[column] = df[column].replace(currency_symbols, regex=True)
    df[column] = pd.to_numeric(df[column], errors='coerce')

print("\n変換後のデータ型:")
print(df.dtypes)
print("\n変換後の最初の数行:")
print(df.head())

# 年間利回りを計算する関数
def calculate_annual_yield(df):
    yields = []
    for i in range(len(df)):
        if df.loc[i, '番号'] < 12:  # 12未満はデータ不足のため計算しない
            yields.append(np.nan)
            print(f"行 {i+1}: 番号 {df.loc[i, '番号']} - データ不足のため計算なし")
        else:
            start_index = max(0, i - 11)
            annual_dividend = df.loc[start_index:i, '配当/株'].sum()
            previous_month_price = df.loc[i - 1, '前月終値']
            
            print(f"行 {i+1}: 番号 {df.loc[i, '番号']}")
            print(f"  年間配当: {annual_dividend}")
            print(f"  前月終値: {previous_month_price}")
            
            if pd.isna(previous_month_price) or previous_month_price == 0:
                yields.append(np.nan)
                print("  前月終値が0またはNaNのため計算不可")
            else:
                yield_value = (annual_dividend / previous_month_price) * 100
                yields.append(yield_value)
                print(f"  利回り: {yield_value}%")
            print("---")
    
    return yields

# 年間利回りの計算と「＄:利回り」列への入力
df['＄:利回り'] = calculate_annual_yield(df)

# 税後利回りの計算と「＄:税後利回り」列への入力
df['＄:税後利回り'] = df['＄:利回り'] * 0.9 * 0.79685

# 結果をCSVファイルに保存
df.to_csv(output_file, index=False)

print(f"\n利回り計算が完了し、結果を{output_file}に保存しました。")

# 最終的な結果の確認
print("\n最終的な利回り計算結果:")
pd.set_option('display.float_format', '{:.6f}'.format)  # 小数点以下6桁まで表示
print(df[['番号', '配当/株', '前月終値', '＄:利回り', '＄:税後利回り']])