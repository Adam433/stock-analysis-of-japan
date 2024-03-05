from utils.config_manager import ConfigManager
from utils.stock_code_list import load_stock_codes
import yfinance as yf
import pandas as pd
import os
from datetime import datetime
from zoneinfo import ZoneInfo

def download_stock_data(stock_codes, config_manager, save_path):
    start_date = config_manager.get_config('download_start_date')
    # end_date = datetime.now(ZoneInfo("Asia/Tokyo")).strftime('%Y-%m-%d')
    end_date = config_manager.get_config('download_end_date')
    for code in stock_codes:
        try:
            data = yf.download(code, start=start_date, end=end_date)
            data.to_csv(os.path.join(save_path, f"{code}.csv"), mode='w')
        except Exception as e:
            print(f"下载失败：{code}, 错误：{e}")

def main():
    config_manager = ConfigManager('config.json')
    save_path = 'data/raw'
    stock_codes = load_stock_codes()
    # stock_codes_test = ["9600.T","7777.T"]
    download_stock_data(stock_codes, config_manager, save_path)

if __name__ == "__main__":
    main()
