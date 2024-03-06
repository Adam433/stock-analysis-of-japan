import pandas as pd
from utils.database import conn  # 导入数据库连接

# =======================================================================
# 将数据库所有的60，120，250交易日涨幅全部更新一遍
# =======================================================================

def calculate_changes(stock_code, df):
    """计算指定DataFrame的60、120和250个交易日的涨跌幅，并确保数据完整性"""
    # 确保 df 已经按日期排序
    df.sort_values(by='date', inplace=True)
    
    # 计算涨跌幅
    df['change_60'] = df['adj_close'].pct_change(periods=60) * 100
    df['change_120'] = df['adj_close'].pct_change(periods=120) * 100
    df['change_250'] = df['adj_close'].pct_change(periods=250) * 100
    
    # 添加股票代码列
    df['stock_code'] = stock_code  
    
    # 不再需要调用fillna，因为在插入数据库时已经处理了None
    # df.fillna(value=None, inplace=True)  # 移除此行
    
    return df[['date', 'change_60', 'change_120', 'change_250', 'stock_code']]

def save_to_normalized_data(df, stock_code):
    """将涨跌幅数据保存或更新到normalized_data表，并每处理10000条数据时打印提示"""
    cursor = conn.cursor()
    # 准备批量插入的数据列表
    data_for_insert = []
    for index, row in df.iterrows():
        data_for_insert.append((
            row['stock_code'], row['date'], 
            None if pd.isna(row['change_60']) else row['change_60'],
            None if pd.isna(row['change_120']) else row['change_120'],
            None if pd.isna(row['change_250']) else row['change_250']
        ))
        
        if len(data_for_insert) == 10000:  # 达到10000条数据时执行批量插入
            cursor.executemany(
                """
                INSERT INTO normalized_data (stock_code, date, change_60, change_120, change_250)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                change_60 = VALUES(change_60),
                change_120 = VALUES(change_120),
                change_250 = VALUES(change_250);
                """, data_for_insert)
            conn.commit()  # 提交事务
            print(f"股票代码 {stock_code}: 已处理 {len(data_for_insert)} 条数据。")
            data_for_insert = []  # 重置数据列表

    if data_for_insert:  # 处理剩余的数据
        cursor.executemany(
            """
            INSERT INTO normalized_data (stock_code, date, change_60, change_120, change_250)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            change_60 = VALUES(change_60),
            change_120 = VALUES(change_120),
            change_250 = VALUES(change_250);
            """, data_for_insert)
        conn.commit()
        print(f"股票代码 {stock_code}: 最终批次已处理 {len(data_for_insert)} 条数据。")

    cursor.close()

def update_normalized_data():
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT stock_code FROM stock_price")
    stock_codes = cursor.fetchall()

    for (stock_code,) in stock_codes:
        cursor.execute("SELECT date, adj_close FROM stock_price WHERE stock_code = %s ORDER BY date", (stock_code,))
        rows = cursor.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=['date', 'adj_close'])
            changes_df = calculate_changes(stock_code, df)
            save_to_normalized_data(changes_df, stock_code)  # 传入股票代码

    cursor.close()

if __name__ == "__main__":
    update_normalized_data()
