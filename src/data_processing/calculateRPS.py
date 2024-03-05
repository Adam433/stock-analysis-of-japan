import pandas as pd
from utils.database import conn

def calculate_and_update_normalized_rankings():
    # 初始化游标，设置为字典类型返回结果以方便处理
    cursor = conn.cursor(dictionary=True)
    update_cursor = conn.cursor()

    try:
        # 从normalized_data表中选择需要处理的记录
        fetch_query = """
        SELECT stock_code, date, change_60, change_120, change_250
        FROM normalized_data
        WHERE change_250 IS NOT NULL
          AND (norm_60 IS NULL OR norm_120 IS NULL OR norm_250 IS NULL)
        """
        cursor.execute(fetch_query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        
        total_records = len(df)  # 待处理的总记录数
        print(f"待处理的总记录数: {total_records}")
        
        records_processed = 0  # 已处理记录的计数器
        
        if not df.empty:
            # 对每个期限的change值进行归一化排名计算
            for period in ['60', '120', '250']:
                change_column = f'change_{period}'
                norm_column = f'norm_{period}'

                # 计算归一化排名
                df[norm_column] = df[change_column].rank(pct=True) * 100

            # 对更新的数据进行批量更新
            for index, row in df.iterrows():
                update_query = """
                UPDATE normalized_data
                SET norm_60 = %s, norm_120 = %s, norm_250 = %s
                WHERE stock_code = %s AND date = %s
                  AND (norm_60 IS NULL OR norm_120 IS NULL OR norm_250 IS NULL)
                """
                update_cursor.execute(update_query, (
                    row['norm_60'], row['norm_120'], row['norm_250'],
                    row['stock_code'], row['date']))
                records_processed += 1
                
                if records_processed % 10000 == 0:
                    print(f"已处理 {records_processed} 条记录...")

            conn.commit()
            print(f"归一化排名更新成功。共更新了 {records_processed} 条记录")
        else:
            print("没有记录需要更新。")

    except Exception as e:
        conn.rollback()  # 发生异常，回滚事务
        print(f"操作过程中发生异常，已回滚事务。异常信息：{e}")
    finally:
        cursor.close()
        update_cursor.close()

if __name__ == "__main__":
    calculate_and_update_normalized_rankings()
