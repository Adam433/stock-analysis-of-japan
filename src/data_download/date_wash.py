import os
import pandas as pd

# 指定包含CSV文件的目录
directory = 'data/raw'

# 遍历目录下的所有文件
for filename in os.listdir(directory):
    if filename.endswith('.csv'):  # 确保只处理CSV文件
        filepath = os.path.join(directory, filename)
        try:
            # 尝试使用pandas读取CSV文件
            df = pd.read_csv(filepath)
            # 如果CSV文件为空（没有数据行），打印文件名
            if df.empty:
                print(f'空文件：{filename}')
        except pd.errors.EmptyDataError:
            # 如果pandas在尝试读取空文件时抛出错误，也打印文件名
            print(f'空文件（读取异常）：{filename}')