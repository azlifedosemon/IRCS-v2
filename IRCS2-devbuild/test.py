import pandas as pd
import openpyxl as pyxl

# Pass a list of names (or indices)
selected: dict[str, pd.DataFrame] = pd.read_excel(
    'D:/1. IRCS Automation/Control 2 DEV/IRCS-v2/IRCS2-devbuild-0.0.1.0/CODE LIBRARY.xlsx',
    sheet_name=['CODE LIBRARY', '>>>', 'UL', 'TRAD']
    # always watch out for backslash when copying path
)

df_code_ul = selected['UL']
df_code_trad = selected['TRAD']

print(df_code_ul)