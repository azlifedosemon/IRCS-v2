import pandas as pd
import re
import sys

INPUT_SHEET_PATH = r'D:\1. IRCS Automation\Control 3 DEV\IRCS-v2\IRCS3_devbuild\Input Sheet_IRCS3.xlsx'

def get_value(key, mydict):
    return mydict.get(key)

def get_local_folder(key, mydict):
    return "\\".join(mydict.get(key).split("\\")[:-1]) 

def get_output_path(key, mydict):
    filename = mydict.get('Output filename')
    proxy = get_local_folder(key, mydict)
    
    return proxy + "\\" + filename
    
def to_list(cell):
    splitter = re.compile(r'[,\;/\\\|\s]+')
    s = str(cell).strip()
    if not s:
        return ''
    parts = splitter.split(s)
    return list(dict.fromkeys(tok.strip() for tok in parts if tok.strip()))
    
def filter_processing(filter_df):
    for col in filter_df.columns:
        if col == 'RunName':
            continue
        filter_df[col] = filter_df[col].apply(to_list)
        
    filters = filter_df.set_index('RunName').to_dict(orient= 'index')

    for run_name, params in filters.items():
        for key in list(params):
            if not key.startswith('only_'):
                continue
            category = key[len('only_'):]              
            only_vals   = set(params.get(f'only_{category}', []))
            excl_vals   = set(params.get(f'exclude_{category}', []))
            clashes     = only_vals & excl_vals
            if clashes:
                if filter_df == FILTER_TRAD:
                    print(
                    f"RECHECK YOUR FILTER_TRAD for '{run_name}': "
                    f"found clash in '{category}' → {sorted(clashes)}"
                    )
                    sys.exit(1)
                else: 
                    print(
                    f"RECHECK YOUR FILTER_UL for '{run_name}': "
                    f"found clash in '{category}' → {sorted(clashes)}"
                    )
                    sys.exit(1)

    return filters


input_sheet     = pd.ExcelFile(INPUT_SHEET_PATH, engine= 'openpyxl')
path_df         = pd.read_excel(input_sheet, 'INPUT_PATH')
FILTER_TRAD     = pd.read_excel(input_sheet, 'FILTER_TRAD').fillna('')
FILTER_UL       = pd.read_excel(input_sheet, 'FILTER_UL').fillna('')
path_map        = dict(zip(path_df['Category'], path_df['Path']))

# Path input
reporting_quarter   = get_value('Reporting Quarter', path_map)
financial_year      = get_value('Financial Year', path_map)
dv_aztrad_csv       = get_value('DV_AZTRAD', path_map)
excel_output        = get_output_path('DV_AZTRAD', path_map)

tradfilter  = filter_processing(FILTER_TRAD)
ulfilter    = filter_processing(FILTER_UL)

print(ulfilter)
print(tradfilter)