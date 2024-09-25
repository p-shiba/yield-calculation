"""
yield.py

ファイル構成:
 /python_lesson/
 ├── config.json
 ├── input.py
 ├── yield.py
 ├── preview.py
 ├── div.csv
 ├── div_processed.csv
 ├── div_yield.csv
 ├── div_preview.csv
 └── run_pipeline.sh

概要:
このスクリプトは、`div_processed.csv`から配当データを読み込み、利回りを計算して`div_yield.csv`として保存します。利回りの計算ロジックは別モジュールで実装されています。

実行フロー:
1. `div_processed.csv`を読み込む。
2. 各行のデータに基づいて利回りを計算する。
3. 計算結果を`div_yield.csv`として保存する。

変更履歴:
2024-09-20:
    - 読み込みファイルをdiv_processed.csvに固定
    - 保存先をdiv_yield.csvに設定
    - 列名消失の問題を修正
    - ファイルの順番と依存関係を明確化
2024-09-24:
    - config.jsonに 'columns_to_convert' キーを追加
2024-09-26:
    - コマンドライン方式を削除して、ダイアログボックスでの入力のみに修正

以前の変更:
    - 利回り計算ロジックの改善
    - エラーハンドリングの強化
"""

# 年間利回りを計算する
# GUIを使用してファイル選択と上書き確認を行う
# スクリプトはダイアログボックスのみを使用し、コマンドライン入力は不要

import pandas as pd
import numpy as np
import json
import logging
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
import os

# ロギングの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file='config.json'):
    """
    設定ファイルを読み込み、必要な設定を返す。
    設定ファイルが存在しない場合や、必要なキーが欠けている場合はデフォルト値を使用する。

    Parameters:
    config_file (str): 設定ファイルのパス

    Returns:
    dict: 設定情報
    """
    default_config = {
        "currency_symbols": {"$": "", "¥": ""},
        "columns_to_convert": ["配当/株", "前月終値"],
        "tax_rate": 0.9 * 0.79685
    }
    try:
        with open(config_file, 'r', encoding='utf-8-sig') as f:
            user_config = json.load(f)
        # デフォルト設定にユーザー設定を上書き
        for key, value in user_config.items():
            default_config[key] = value
        # 必要なキーが存在するか確認し、なければデフォルトを使用
        for key in default_config:
            if key not in user_config:
                logging.warning(f"設定ファイルにキー '{key}' が存在しません。デフォルト値を使用します。")
        return default_config
    except FileNotFoundError:
        logging.warning(f"設定ファイル {config_file} が見つかりません。デフォルト設定を使用します。")
        return default_config
    except json.JSONDecodeError:
        logging.error(f"設定ファイル {config_file} の形式が正しくありません。デフォルト設定を使用します。")
        return default_config

def calculate_annual_yield(df):
    """
    DataFrame に年間利回りを計算して追加する。

    Parameters:
    df (pd.DataFrame): 配当データを含むデータフレーム

    Returns:
    pd.Series: 計算された年間利回り
    """
    # データが '番号' でソートされていることを確認
    df = df.sort_values('番号').reset_index(drop=True)
    
    # 12ヶ月の配当合計を計算
    df['annual_dividend'] = df['配当/株'].rolling(window=12).sum()
    
    # 前月終値を取得
    df['previous_month_price'] = df['前月終値'].shift(1)
    
    # 利回りの計算
    df['＄:利回り'] = (df['annual_dividend'] / df['previous_month_price']) * 100
    
    # 番号が12未満の行と前月終値がNaNまたは0の行はNaNに設定
    df.loc[df['番号'] < 12, '＄:利回り'] = np.nan
    df.loc[df['previous_month_price'].isna() | (df['previous_month_price'] == 0), '＄:利回り'] = np.nan
    
    # 必要に応じてログを出力
    logging.info("利回り計算が完了しました。")
    
    return df['＄:利回り']

def select_file(save=False, default_name='div_yield.csv'):
    """
    ファイル選択ダイアログを表示し、ファイルパスを取得する。

    Parameters:
    save (bool): 保存ダイアログを表示するかどうか
    default_name (str): 保存ダイアログでデフォルトで表示されるファイル名

    Returns:
    str: 選択されたファイルのパス
    """
    root = tk.Tk()
    root.withdraw()  # メインウィンドウを非表示
    if save:
        initial_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else Path.home()
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=initial_dir,
            initialfile=default_name
        )
    else:
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    root.destroy()
    return file_path

def confirm_overwrite(output_file):
    """
    出力ファイルが既に存在する場合に上書きするか確認するダイアログを表示する。

    Parameters:
    output_file (str): 出力ファイルのパス

    Returns:
    bool: 上書きする場合はTrue、しない場合はFalse
    """
    root = tk.Tk()
    root.withdraw()  # メインウィンドウを非表示
    result = messagebox.askyesno("確認", f"出力ファイル '{output_file}' は既に存在します。上書きしますか？")
    root.destroy()
    return result

def main():
    config = load_config()
    
    # 入力ファイルの選択
    input_file = select_file(save=False)
    if not input_file:
        logging.error("入力ファイルが選択されませんでした。")
        return
    
    # 出力ファイルの選択
    output_file = select_file(save=True, default_name='div_yield.csv')
    if not output_file:
        logging.error("出力ファイルが選択されませんでした。")
        return
    
    # 出力ファイルが既に存在する場合の確認
    if os.path.exists(output_file):
        overwrite = confirm_overwrite(output_file)
        if not overwrite:
            logging.info("処理を中断しました。")
            return
    
    # データの読み込み
    try:
        df = pd.read_csv(input_file, encoding='utf-8-sig')
    except FileNotFoundError:
        logging.error(f"入力ファイル '{input_file}' が見つかりません。")
        return
    except pd.errors.EmptyDataError:
        logging.error(f"入力ファイル '{input_file}' が空です。")
        return
    except pd.errors.ParserError as e:
        logging.error(f"入力ファイル '{input_file}' の解析中にエラーが発生しました: {e}")
        return
    
    logging.info("元のデータ型:")
    logging.info(df.dtypes)
    logging.info("\n最初の数行:")
    logging.info(df.head())
    
    # 必要な列が存在するか確認
    missing_columns = [col for col in config['columns_to_convert'] if col not in df.columns]
    if missing_columns:
        logging.error(f"指定された列 {missing_columns} が入力ファイルに存在しません。")
        return
    
    # 列のデータクレンジング
    for column in config['columns_to_convert']:
        df[column] = df[column].replace(config['currency_symbols'], regex=True)
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    logging.info("\n変換後のデータ型:")
    logging.info(df.dtypes)
    logging.info("\n変換後の最初の数行:")
    logging.info(df.head())
    
    # 利回り計算
    df['＄:利回り'] = calculate_annual_yield(df)
    df['＄:税後利回り'] = df['＄:利回り'] * config['tax_rate']
    
    # データの保存
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"\n利回り計算が完了し、結果を '{output_file}' に保存しました。")
    except Exception as e:
        logging.error(f"出力ファイル '{output_file}' の保存中にエラーが発生しました: {e}")
        return
    
    # 最終結果のログ出力
    logging.info("\n最終的な利回り計算結果:")
    pd.set_option('display.float_format', '{:.6f}'.format)
    logging.info(df[['番号', '配当/株', '前月終値', '＄:利回り', '＄:税後利回り']])

if __name__ == "__main__":
    main()
