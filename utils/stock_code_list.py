import pandas as pd

def load_stock_codes(xls_path = 'data/data_j_wash.xlsx',sheet_name='Sheet1', exclude_category="ETF・ETN"):
    """
    从指定的Excel文件加载股票代码列表。

    参数:
    - xls_path: Excel文件的路径。
    - sheet_name: Excel中的工作表名称。
    - exclude_category: 要排除的市场商品区分，例如"ETF・ETN"。

    返回:
    - 一个包含股票代码的列表。
    """
    df = pd.read_excel(xls_path,sheet_name )
    stock_codes = [f"{row['コード']}.T" for index, row in df.iterrows() if row['市場・商品区分'] != exclude_category]
    return stock_codes

def main():
    stock_codes = load_stock_codes()
    print(stock_codes)
if __name__ == "__main__":
    main()