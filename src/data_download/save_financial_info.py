import datetime
import pandas as pd
import yfinance as yf
from utils.database import conn  # 假设这是从utils.database模块导入的现有数据库连接对象

def fetch_all_stock_codes():
    """从数据库的stock_info表中检索所有股票代码。"""
    try:
        with conn.cursor() as cursor:
            query = "SELECT stock_code FROM stock_info"
            cursor.execute(query)
            stock_codes_tuples = cursor.fetchall()
            stock_codes = [code[0] for code in stock_codes_tuples]  # 提取每个元组的第一个元素
            stock_codes =["6966"]
        return stock_codes
    except Exception as e:
        print(f"从数据库检索股票代码时发生错误: {e}")
        return []


def download_financial_data(stock_code):
    """下载指定股票代码的财务数据并保存到数据库中。"""
    try:
        stock = yf.Ticker(f"{stock_code}.T")  # 假设是日本股票，加上.T后缀
        financials = stock.financials
        if not financials.empty:
            save_financial_data_to_db(stock_code, financials)
        else:
            print(f"No financial data found for {stock_code}")
    except Exception as e:
        print(f"下载或保存股票代码{stock_code}的财务数据时发生错误: {e}")

def save_financial_data_to_db(stock_code, financials):
    """将财务数据保存到数据库中。"""
    connection = conn
    cursor = connection.cursor()

    # 财务指标到数据库列的映射
    metric_to_column = {
        'Net Income': 'net_income',
        'Operating Income': 'operating_income',
        'Gross Profit': 'gross_profit',
        'EBITDA': 'ebitda',
        'Basic EPS': 'eps_basic',  # 假设在数据中以这种形式存在
        'Diluted EPS': 'eps_diluted',  # 同上
        'Operating Expense': 'operating_expense',
        'Cost Of Revenue': 'cost_of_revenue',
        'Pretax Income': 'pretax_income',
        'Tax Provision': 'tax_provision',
        'Total Revenue': 'total_revenue',
        'Operating Revenue': 'operating_revenue',
    }

    # 遍历DataFrame的每一行
    for metric, row in financials.iterrows():
        column_name = metric_to_column.get(metric)
        if not column_name:
            continue

        # 遍历每个日期（列）
        for report_date, value in row.items():
            # 直接从Timestamp转换为date，无需转换为字符串
            if isinstance(report_date, pd.Timestamp):
                report_date = report_date.date()
            else:
                # 如果不是Timestamp，尝试转换
                report_date = pd.to_datetime(report_date).date()

            sql = f"""
                INSERT INTO financial_data
                (stock_code, report_date, {column_name})
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                {column_name} = VALUES({column_name});
            """
            
            try:
                cursor.execute(sql, (stock_code, report_date, value))
            except Exception as e:
                print(f"Error saving data for {stock_code}, metric {metric}, on {report_date}: {e}")
                connection.rollback()  # 发生错误时回滚事务
                continue

    connection.commit()  # 提交事务
    cursor.close()
    print(f"Data for {stock_code} saved successfully.")


def main():
    stock_codes = fetch_all_stock_codes()
    if stock_codes:
        for stock_code in stock_codes:
            download_financial_data(stock_code)
    else:
        print("没有找到任何股票代码进行更新。")

if __name__ == "__main__":
    main()
