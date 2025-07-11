import pandas as pd

print('START')
INPUT_SHEET_PATH = r'D:\1. IRCS Automation\Control 3 DEV\IRCS-v2\IRCS3_devbuild\Input Sheet_IRCS3.xlsx'

path_df = pd.read_excel(INPUT_SHEET_PATH, engine='openpyxl', sheet_name=['INPUT_PATH'])
path_map = dict(zip(path_df['Category'], path_df['Path']))
print(path_map)