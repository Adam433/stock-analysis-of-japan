import os
import pandas as pd
from utils.database import conn
# =======================================================================
# 从各个股票的csv里面导出所有的价格变动和日期信息存到数据库
# TODO 涨跌幅存到归一化表
# =======================================================================
# 存放CSV文件的目录路径
csv_directory = 'data/raw'

def calculate_changes(df):
    """计算60、120和250交易日的涨跌幅"""
    df.sort_values(by='Date', inplace=True)
    for days in [60, 120, 250]:
        df[f'change_{days}'] = df['Adj Close'].pct_change(periods=days) * 100
    return df

def save_to_database(conn, stock_code, df):
    """将数据保存到数据库，处理NaN值为None（SQL中的NULL）"""
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO stock_price (stock_code, date, open, high, low, close, adj_close, volume, change_60, change_120, change_250) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for index, row in df.iterrows():
        # 转换NaN值为None
        row_data = (
            stock_code, 
            row['Date'], row['Open'], row['High'], row['Low'], row['Close'], 
            row['Adj Close'], row['Volume'], 
            row.get('change_60'), row.get('change_120'), row.get('change_250')
        )
        cleaned_data = tuple([None if pd.isna(x) else x for x in row_data])
        cursor.execute(insert_sql, cleaned_data)
    conn.commit()
    cursor.close()

def process_files(csv_directory):
    """处理目录下的所有CSV文件"""
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_directory, filename)
            df = pd.read_csv(file_path)
            df.fillna(value={'change_60': None, 'change_120': None, 'change_250': None}, inplace=True)  # 处理NaN值
            df = calculate_changes(df)
            stock_code = filename[:-6]  # 假设文件名格式为"股票代码.T.csv"
            save_to_database(conn, stock_code, df)

if __name__ == "__main__":
    process_files(csv_directory)
