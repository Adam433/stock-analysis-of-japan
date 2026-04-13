import pandas as pd
from utils.database import conn

# 从xlsx文件中导入数据
df = pd.read_excel('data/data_j.xls', usecols=[1, 2, 3, 5, 9], header=0)
df.columns = ['stock_code', 'company_name', 'product_type', 'industry', 'market_scale']

# 过滤数据，移除不需要的类型
df = df[df['product_type'] != 'ETF・ETN']

# 准备SQL语句
insert_update_sql = """
    INSERT INTO stock_info (stock_code, company_name, industry, market_scale) 
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE company_name=VALUES(company_name), industry=VALUES(industry), market_scale=VALUES(market_scale)
"""

try:
    # 连接数据库并获取cursor
    cursor = conn.cursor()

    # 插入或更新数据
    for index, row in df.iterrows():
        cursor.execute(insert_update_sql, (row['stock_code'], row['company_name'], row['industry'], row['market_scale']))

    # 提交事务
    conn.commit()
except Exception as e:
    print(f"数据库操作出错: {e}")
finally:
    # 关闭游标和连接
    cursor.close()
    conn.close()
