from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import yfinance as yf
import pandas as pd
from utils.database import conn  # 导入数据库连接
from utils.config_manager import ConfigManager  # 导入配置管理器

# 初始化配置管理器
config_manager = ConfigManager('config.json')  # 确保这里的路径是正确的

def get_last_date_from_db(stock_code):
    """从MySQL数据库中获取给定股票代码的最新日期。"""
    try:
        cursor = conn.cursor()
        query = "SELECT MAX(date) FROM stock_price WHERE stock_code = %s"
        cursor.execute(query, (stock_code,))
        result = cursor.fetchone()
        cursor.close()
        if result and result[0]:
            return result[0].strftime('%Y-%m-%d')
    except Exception as e:
        print(f"从数据库获取日期时发生错误: {e}")
    return None

def save_to_database(stock_code, df):
    """将数据保存到数据库，适用于MySQL，并处理NaN值为None（SQL中的NULL）。"""
    try:
        cursor = conn.cursor()
        insert_stock_price_sql = """INSERT INTO stock_price 
                                    (stock_code, date, open, high, low, close, adj_close, volume) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        for index, row in df.iterrows():
            cursor.execute(insert_stock_price_sql, (
                stock_code, 
                row['date'], row['open'], row['high'], row['low'], 
                row['close'], row['adj_close'], row['volume']
            ))
        conn.commit()  # 提交事务
    except Exception as e:
        conn.rollback()  # 回滚事务
        print(f"保存到数据库时发生错误: {e}")
    finally:
        cursor.close()

def update_stock_data_in_db(stock_codes):
    today = datetime.now(ZoneInfo("Asia/Tokyo"))
    end_date = today + timedelta(days=1)
    today_date_str = today.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # 从配置文件获取默认起始日期
    default_start_date = config_manager.get_config('download_start_date')
    
    failed_downloads = []  # 用于跟踪下载失败的股票代码

    for code in stock_codes:
        yf_code = f"{code}.T"
        last_date_str = get_last_date_from_db(code)
        
        if last_date_str == today_date_str:
            print(f"{code} 已更新到最新状态。")
            continue

        start_date = last_date_str if last_date_str else default_start_date
        try:
            data = yf.download(yf_code, start=start_date, end=end_date_str)
            if not data.empty:
                df = pd.DataFrame(data)
                df.reset_index(inplace=True)
                df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low",
                                   "Close": "close", "Adj Close": "adj_close", "Volume": "volume"}, inplace=True)
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                if last_date_str:
                    df = df[df['date'] > last_date_str]
                if not df.empty:
                    # save_to_database(code, df)  # 使用原始股票代码保存到数据库
                    print(df)
                    print(f"数据更新完成：{code}")
            else:
                failed_downloads.append(code)
        except Exception as e:
            print(f"尝试下载 {code} 的数据时出错: {e}")
            failed_downloads.append(code)

    # 如果有下载失败的股票代码，打印它们
    if failed_downloads:
        print(f"以下股票代码的数据下载失败: {', '.join(failed_downloads)}")

def fetch_all_stock_codes():
    """从数据库的stock_info表中检索所有股票代码"""
    try:
        cursor = conn.cursor()
        query = "SELECT stock_code FROM stock_info"
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        # 提取股票代码并返回一个列表
        stock_codes = [row[0] for row in result]
        return stock_codes
    except Exception as e:
        print(f"从数据库检索股票代码时发生错误: {e}")
        return []


def main():
    stock_codes = fetch_all_stock_codes()  # 从数据库中获取所有股票代码
    # stock_codes = ["1301"] # 下载失败的时候在这里添加
    if stock_codes:
        update_stock_data_in_db(stock_codes)
    else:
        print("没有找到任何股票代码进行更新。")

if __name__ == "__main__":
    main()

