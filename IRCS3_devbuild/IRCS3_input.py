import pandas as pd
import re
import sys
import time
from pathlib import Path

start_time = time.time()


# ENTER THE SHEET PATH HERE

INPUT_SHEET_PATH = r'D:\1. IRCS Automation\Control 3 DEV\IRCS3_devbuild\Input Sheet_IRCS3.xlsx'












def get_value(key, mydict):
    return mydict.get(key)

def get_local_folder(key, mydict):
    return "\\".join(mydict.get(key).split("\\")[:-1]) 

def get_output_path(filekey, key, mydict):
    filename = mydict.get(filekey)
    proxy = get_local_folder(key, mydict)
    
    return proxy + "\\" + filename + '.xlsx'
    
def to_list(cell):
    splitter = re.compile(r'[,\;/\\\|\s]+')
    s = str(cell).strip()
    if not s:
        return ''
    parts = splitter.split(s)
    return list(dict.fromkeys(tok.strip() for tok in parts if tok.strip()))
    
def normalize_cohort_token(tok):
    """
    Turn things like '2024.0' → '2024', but leave other tokens alone.
    """
    s = str(tok).strip()
    # match an integer with optional .0, .00, etc.
    m = re.fullmatch(r'(\d+)(?:\.0+)?', s)
    return m.group(1) if m else s
   
def filter_processing(filter_df):
    for col in filter_df.columns:
        if col == 'run_name':
            continue
        elif col in ['path_dv','path_rafm', 'path_uvsg']:
            filter_df[col] = filter_df[col].astype(str).str.strip().replace('', pd.NA)
        elif col.strip().upper() == 'USDIDR':
            # Convert blank → NaN, then to float
            filter_df[col] = pd.to_numeric(
                filter_df[col].astype(str).str.strip().replace('', pd.NA),
                errors='coerce'
            )
        else:
            lst = filter_df[col].apply(to_list)

            # but if this is one of the cohort columns, normalize out any .0
            if col in ('only_cohort', 'exclude_cohort'):
                lst = lst.apply(lambda toks: [ normalize_cohort_token(tok) for tok in toks ])

            filter_df[col] = lst
    
    filter_df.columns = (
    filter_df.columns
    .str.strip()
    )
    
    name = ''
    
    if filter_df is FILTER_TRAD:
        name = 'FILTER_TRAD'
    elif filter_df is FILTER_UL:
        name = 'FILTER_UL'
    
    missing_print = []
    
    if name == 'FILTER_UL':
        missing_path_dv = filter_df.loc[
            filter_df['path_dv'].isna(), 'run_name'
        ].tolist()
        if missing_path_dv:
            missing_print.append(f"ERROR PATH: fill path_dv for {', '.join(missing_path_dv)} in {name}",)
            
    missing_path_rafm = filter_df.loc[
        filter_df['path_rafm'].isna(), 'run_name'
    ].tolist()
    if missing_path_rafm:
        missing_print.append(f"ERROR PATH: fill path_rafm for {', '.join(missing_path_rafm)} in {name}",)
                
    missing_rate = filter_df.loc[
        filter_df['USDIDR'].isna(), 'run_name'
    ].tolist()
    if missing_rate:
        missing_print.append(f"ERROR RATE: fill USDIDR rate for {', '.join(missing_rate)} in {name}",)
    
    if missing_print:
        print()
        for msg in missing_print:
            if msg: 
                print(msg)
        print()
            
    path_error = False
    if not missing_path_rafm:
        for col in ['path_rafm']:
            for run, path_str in zip(filter_df['run_name'], filter_df[col]):
                p = Path(path_str)
                if not p.exists():
                    print(f"ERROR FILEPATH: {col} for {run} in {name} not found at:\n  → {p}")
                    path_error = True
                
    missing = missing_path_rafm or missing_rate or path_error
    
    filters = filter_df.set_index('run_name').to_dict(orient= 'index')

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
        print("ERROR CLASH: Clash in value!")
        print(f"For {sheet} in Input Sheet_IRCS3.xlsx recheck: \n")
        for run_name, cat_map in run_clashes.items():
            details = "; ".join(
                f"{cat} -> {', '.join(sorted(vals))}"
                for cat, vals in cat_map.items()
            )
            print(f"• {run_name} in {name}: clash in {details}")
        sys.exit(1)

    return filters


input_sheet     = pd.ExcelFile(INPUT_SHEET_PATH, engine= 'openpyxl')
path_df         = pd.read_excel(input_sheet, 'INPUT_SETTING')
FILTER_TRAD     = pd.read_excel(input_sheet, 'FILTER_TRAD').fillna('')
FILTER_UL       = pd.read_excel(input_sheet, 'FILTER_UL').fillna('')
VARIABLE_DEF    = pd.read_excel(input_sheet, 'VARIABLE_DEF')
var_map         = dict(zip(VARIABLE_DEF['Variable Name'], VARIABLE_DEF['Options']))
path_map        = dict(zip(path_df['Category'], path_df['Path']))

# Path input
valuation_month     = str(get_value('Valuation Month', path_map)) + 'M'
valuation_year      = get_value('Valuation Year', path_map)
valuation_rate      = get_value('FX Rate Valdate', path_map)
product_model       = get_value('Product Model', path_map)
dv_aztrad_csv       = get_value('DV_AZTRAD', path_map)

excel_output_trad   = get_output_path('Output Trad', 'DV_AZTRAD', path_map)
excel_output_ul     = get_output_path('Output UL', 'DV_AZUL', path_map)

# Fetch Filter
ulfilter    = filter_processing(FILTER_UL)
tradfilter  = filter_processing(FILTER_TRAD)
        
        
bool_trad = False
bool_ul   = False

if tradfilter:
    bool_trad = True
 
if ulfilter:
    bool_ul = True

# Fetch Variable
option_channel      = [chnl for chnl in get_value('channel', var_map).split(',')]
option_currency     = [curr for curr in get_value('currency', var_map).split(',')]
option_portfolio    = [por for por in get_value('portfolio', var_map).split(',')]
option_period       = [period for period in get_value('period', var_map).split(',')]

end_time = time.time()
