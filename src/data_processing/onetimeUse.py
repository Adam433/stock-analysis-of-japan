from utils.database import conn

def transfer_data_and_calculate_normalized():
    cursor = conn.cursor(buffered=True)
    insert_cursor = conn.cursor(buffered=True)

    # 查询原始表中的总记录数
    cursor.execute("SELECT COUNT(*) FROM stock_price")
    total_records = cursor.fetchone()[0]
    print(f"Total records to process: {total_records}")

    # 查询需要转存的数据
    fetch_query = """
    SELECT stock_code, date, change_60, change_120, change_250
    FROM stock_price
    """
    cursor.execute(fetch_query)

    # 准备插入到目标表的SQL语句
    insert_query = """
    INSERT INTO normalized_data (stock_code, date, change_60, change_120, change_250)
    VALUES (%s, %s, %s, %s, %s)
    """

    count = 0
    try:
        for (stock_code, date, change_60, change_120, change_250) in cursor:
            # 这里直接将数据转存，实际应用中可能需要根据实际情况进行数据处理或转换
            insert_cursor.execute(insert_query, (stock_code, date, change_60, change_120, change_250))
            count += 1

            # 每处理10000条数据就输出一次提示
            if count % 10000 == 0:
                print(f"Processed {count} records...")
                conn.commit()  # 提交事务以确保数据被保存

        conn.commit()  # 确保所有剩余的数据都被提交
        print("Data transfer completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # 如果出现错误，回滚事务
    finally:
        cursor.close()
        insert_cursor.close()

if __name__ == "__main__":
    transfer_data_and_calculate_normalized()
