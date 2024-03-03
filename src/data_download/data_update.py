import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from utils.config_manager import ConfigManager

def get_last_date_from_csv(file_path):
    """从CSV文件中读取最后一行的日期。"""
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            if not df.empty:
                # 确保读取的是字符串类型的日期
                last_row = df.iloc[-1]
                last_date_str = str(last_row['Date'])
                # 尝试转换为日期格式，如果失败则返回None
                datetime.strptime(last_date_str, '%Y-%m-%d')
                return last_date_str
    except Exception as e:
        print(f"处理CSV文件日期时发生错误: {e}")
    return None

def update_stock_data(stock_codes, config_manager, save_path):
    today_date = datetime.now(ZoneInfo("Asia/Tokyo")).strftime('%Y-%m-%d')
    for code in stock_codes:
        file_path = os.path.join(save_path, f"{code}.csv")
        start_date = get_last_date_from_csv(file_path)
        
        # 如果文件不存在或找不到起始日期，使用config中的update_date
        if start_date is None:
            start_date = config_manager.get_config('update_date')
        
        try:
            data = yf.download(code, start=start_date, end=today_date)
            if not data.empty:
                # 如果下载的数据只有一条且日期与现有数据的最后一条相同，则跳过
                if len(data) > 1 or (len(data) == 1 and data.index[0].strftime('%Y-%m-%d') != start_date):
                    # 如果下载数据的第一条日期与start_date相同，则去除第一条
                    if data.index[0].strftime('%Y-%m-%d') == start_date:
                        data = data.iloc[1:]
                    data.to_csv(file_path, mode='a', header=False)
                    print(f"数据更新完成：{code}")
        except Exception as e:
            print(f"更新失败：{code}, 错误：{e}")
    # 更新配置文件中的update_date为今天的日期
    config_manager.update_config('update_date', today_date)

def main():
    config_manager = ConfigManager(os.path.join('config.json'))
    save_path = os.path.join('data', 'raw')
    xls_path = os.path.join('data', 'data_j.xls')
    df = pd.read_excel(xls_path, sheet_name='Sheet1')
    stock_codes = [f"{row['コード']}.T" for index, row in df.iterrows() if row['市場・商品区分'] != "ETF・ETN"]
    # stock_codes_test = ["9600.T","7777.T"]
    update_stock_data(stock_codes, config_manager, save_path)

if __name__ == "__main__":
    main()
