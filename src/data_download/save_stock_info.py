import pandas as pd
from utils.database import conn
# =======================================================================
# 从xlsx里面导出所有的股票信息存到数据库
# =======================================================================
# 修改这里，使用列的位置索引，并指定header=0以使用第一行作为列名
df = pd.read_excel('data/data_j_wash.xlsx', usecols=[1, 2, 3, 5, 9], header=0)

# 修改这里，根据你的Excel文件的实际列名进行筛选和重命名
# 假设经过header=0处理后，列名自动对应于B、C、D、F、J列的内容
filtered_df = df[df[df.columns[2]] != 'ETF・ETN'].copy()

# 重命名列以匹配数据库表字段，这里根据实际情况可能需要调整列名
filtered_df.columns = ['stock_code', 'company_name', 'product_type', 'industry', 'market_scale']

# 准备插入数据的SQL语句
insert_sql = """
    INSERT INTO stock_info (stock_code, company_name, industry, market_scale) 
    VALUES (%s, %s, %s, %s)
"""

try:
    cursor = conn.cursor()
    # 批量插入数据
    for index, row in filtered_df.iterrows():
        cursor.execute(insert_sql, (row['stock_code'], row['company_name'], row['industry'], row['market_scale']))
    
    # 提交事务
    conn.commit()
except Exception as e:
    print(f"数据库操作出错: {e}")
finally:
    # 关闭游标和连接
    cursor.close()
    conn.close()
