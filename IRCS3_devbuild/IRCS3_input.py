import pandas as pd
import re
import sys
import time

start_time = time.time()

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
        elif col.strip().upper() == 'USDIDR':
            # Convert blank → NaN, then to float
            filter_df[col] = pd.to_numeric(
                filter_df[col].astype(str).str.strip().replace('', pd.NA),
                errors='coerce'
            )
        else:
            filter_df[col] = filter_df[col].apply(to_list)
    
    filter_df.columns = (
    filter_df.columns
    .str.strip()
    )
    
    missing = filter_df.loc[
        filter_df['USDIDR'].isna(), 'RunName'
    ].tolist()
    if missing:
        print(f"\nERROR: fill USDIDR rate for {', '.join(missing)}",)
        print()
        
    filters = filter_df.set_index('RunName').to_dict(orient= 'index')

    run_clashes = {}
    for run_name, params in filters.items():
        for key, only_list in params.items():
            if not key.startswith('only_'):
                continue
            category  = key[len('only_'):]              
            excl_list = params.get(f'exclude_{category}', [])
            common    = set(only_list) & set(excl_list)
            if common:
                run_clashes.setdefault(run_name, {})\
                           .setdefault(category, set())\
                           .update(common)

    if run_clashes or missing:
        if missing and not run_clashes:
            sys.exit(1)
        sheet = 'FILTER_TRAD' if filter_df is FILTER_TRAD else 'FILTER_UL'
        print("ERROR: Clash in value!")
        print(f"For {sheet} in Input Sheet_IRCS3.xlsx recheck: \n")
        for run_name, cat_map in run_clashes.items():
            details = "; ".join(
                f"{cat} -> {', '.join(sorted(vals))}"
                for cat, vals in cat_map.items()
            )
            print(f"• {run_name}: clash in {details}")
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

ulfilter    = filter_processing(FILTER_UL)
tradfilter  = filter_processing(FILTER_TRAD)
end_time = time.time()