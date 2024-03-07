import configparser
import mysql.connector
import sys

from utils.config_manager import ConfigManager

# 读取配置文件
config = configparser.ConfigParser()
config.read('config.ini')
# 初始化配置管理器
config_manager = ConfigManager('config.json')  # 确保这里的路径是正确的

# 建立数据库连接
conn = mysql.connector.connect(
    host=config['mysql']['host'],
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database=config['mysql']['database']
)

# 从配置文件获取配置项
growth_rate_threshold = config_manager.get_config('growth_rate_threshold') or 0.25
consecutive_years = config_manager.get_config('consecutive_years') or 3

# 可选地从命令行参数覆盖配置
if len(sys.argv) > 1:
    consecutive_years = int(sys.argv[1])
if len(sys.argv) > 2:
    growth_rate_threshold = float(sys.argv[2])

# 查询最新年份的SQL
latest_year_sql = """
SELECT stock_code, MAX(YEAR(report_date)) AS latest_year
FROM financial_data
GROUP BY stock_code
"""

qualified_stocks = []

# 假设其他部分代码不变，直接从尝试获取每个股票的财务数据开始

try:
    with conn.cursor(dictionary=True) as cursor:
        cursor.execute(latest_year_sql)
        latest_years = cursor.fetchall()
        
        for record in latest_years:
            stock_code = record['stock_code']
            latest_year = record['latest_year']
            
            check_growth_sql = """
            SELECT stock_code, YEAR(report_date) AS year, net_income
            FROM financial_data
            WHERE stock_code = %s AND YEAR(report_date) BETWEEN %s AND %s
            ORDER BY year ASC
            """
            
            start_year = latest_year - consecutive_years
            
            cursor.execute(check_growth_sql, (stock_code, start_year, latest_year))
            results = cursor.fetchall()

            if len(results) != consecutive_years+1:
                continue

            # 初始化变量以检查连续增长和满足增长率
            meets_criteria = True
            previous_year_net_income = None

            for result in results:
                if previous_year_net_income is not None:
                    # 检查连续增长
                    if result['net_income'] <= previous_year_net_income:
                        meets_criteria = False
                        break
                    # 计算增长率
                    growth_rate = (result['net_income'] - previous_year_net_income) / previous_year_net_income
                    if growth_rate < growth_rate_threshold:
                        meets_criteria = False
                        break
                
                previous_year_net_income = result['net_income']

            if meets_criteria:
                qualified_stocks.append(stock_code)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    conn.close()

# 输出符合条件的股票
for stock_code in qualified_stocks:
    print(f"Stock Code: {stock_code} meets the growth criteria.")


