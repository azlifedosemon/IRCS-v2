import pandas as pd
import openpyxl as pyxl

# Pass a list of names (or indices)
selected: dict[str, pd.DataFrame] = pd.read_excel(
    'IRCS/Control 2/CODE LIBRARY.xlsx',
    sheet_name=['CODE LIBRARY', '>>>', 'UL', 'TRAD']
)

df_code_ul = selected['UL']
df_code_trad = selected['TRAD']

print(df_code_ul)