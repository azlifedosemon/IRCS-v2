import pandas as pd
import re
import time
import subprocess
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import IRCS3_input as input_sheet

start_time = time.time()


# DV funct
def elapsed_time(start,end,script):
    if round((end - start),0) > 60:
        print(f"\n• {script} RUNTIME: {round((end - start) / 60, 2)} minutes", end='')
    elif (end - start) < 1:
        print(f"\n• {script} RUNTIME: {round((end - start) * 1000, 2)} ms", end= '')
    else:
        print(f"\n• {script} RUNTIME: {round((end - start), 2)} second", end= '')

filtered_runs = {}
usdidr        = {}

def filtered_df(df, filter_dict, run_name):
    goc_upper = df['goc'].astype(str).str.upper()

    params = filter_dict[run_name]
    mask = pd.Series(True, index=df.index)
    rate = params.get('USDIDR')
    if rate is not None:
        usdidr[run_name] = rate

    # Helper: Split tokens if given as comma-separated string
    def tokens_list(x):
        if isinstance(x, list):
            return x
        if not x:
            return []
        return [t.strip() for t in str(x).split(',') if t.strip()]

    # Exclude filters
    for key in ('exclude_channel', 'exclude_currency', 'exclude_portfolio', 'exclude_cohort', 'exclude_period'):
        tokens = tokens_list(params.get(key))
        if tokens:
            pat = '|'.join(fr'(?:^|_){re.escape(tok.upper())}(?:_|$)' for tok in tokens)
            mask &= ~goc_upper.str.contains(pat, na=False, regex=True)


    # Only filters (if set)
    for key in ('only_channel', 'only_currency', 'only_portfolio', 'only_cohort', 'only_period'):
        tokens = tokens_list(params.get(key))
        if tokens:
            pat = '|'.join(fr'(?:^|_){re.escape(tok.upper())}(?:_|$)' for tok in tokens)
            mask &= goc_upper.str.contains(pat, na=False, regex=True)

    filtered_runs[run_name] = df.loc[mask]

    return 


def clean_goc(filter_dict, run_name):
    
    period = bool(filter_dict[run_name]['only_period'])
    
    def clean_goc_inner(name, period= period):
        
        tokens = [tok for tok in name.split('_') if tok]
        
        start_index = ''
        year_index  = ''
        prod_index  = ''
        
        for i_, x in enumerate(tokens):
            if x in input_sheet.option_channel:
                start_index = int(i_)
                if period:
                    break
            else:
                if x in ['H', 'L']:
                    prod_index = int(i_)
                    if period:
                        break
            if x.isnumeric():
                year_index = int(i_)
        
        if period:
            if start_index:
                return "_".join(tokens[start_index:])
            else:
                return "_".join(tokens[prod_index:])
                
        if year_index:
            year_index += 1
            if start_index:
                return "_".join(tokens[start_index: year_index])
            else:
                return "_".join(tokens[prod_index: year_index])
        
    return clean_goc_inner
 

def build_cleaned_runs(filtered_df, usdidr, filter):
    cleaned_runs = {}

    for run_name, dv_df in filtered_df.items():
        # 1) Work on a fresh copy
        df = dv_df.copy()

        # 2) Clean the GOC codes
        sorter = clean_goc(filter, run_name)
        df.loc[:, 'goc'] = df['goc'].apply(sorter)
        # 2b) Special fix for IDR_NO_2025
        df.loc[:, 'goc'] = df['goc'].replace('IDR_NO_2025', 'H_IDR_NO_2025')

        # 3) Fix up numeric columns in one go (no intermediate string assignment)
        df.loc[:, 'pol_num'] = pd.to_numeric(
            df['pol_num'].astype(str)
                       .str.replace(',', '.', regex=False),
            errors='coerce'
        )
        df['total_fund'] = pd.to_numeric(
            df['total_fund'].astype(str)
                .str.replace(',', '.', regex=False),
            errors='coerce'
        )
        
        df = df.groupby(['goc'], as_index= False).sum(numeric_only = True)
        
        usd_mask = df['goc'].str.contains('USD',case = False,na = False)
        df.loc[usd_mask, 'total_fund'] = df.loc[usd_mask, 'total_fund'] * usdidr[run_name]
        
        # 4) Store back into the new dict
        cleaned_runs[run_name] = df

    return cleaned_runs


def load_dv_excels(ulfilter):
    """
    For a dict of run configs, returns a dict: path_dv -> DataFrame,
    ensuring each file is read only once.
    """
    cache = {}
    for run_params in ulfilter.values():
        path = run_params['path_dv']
        if path not in cache:
            # Read Excel as CSV (assuming first sheet, or name if you know it)
            df = pd.read_excel(path, engine='openpyxl')
            # Mirror your CSV read: drop unnecessary columns
            cols_to_drop = (['product_group', 'pre_ann', 'loan_sa']
                            + [c for c in df.columns if str(c).startswith('Unnamed')])
            cache[path] = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
            
    return cache


def run_dv_worker(path):
    """
    Call the DV worker subprocess for a single file path.
    Returns (path, DataFrame).
    """
    # Use sys.executable for venv safety
    out_pkl = str(Path.cwd() / f"dv_{Path(path).stem}.pkl")
    subprocess.check_call([sys.executable, str(WORKER), path, out_pkl])
    return path, pd.read_pickle(out_pkl)


def build_dv_subprocess(paths, max_workers):
    """
    Submit all unique paths to worker, gather DataFrames.
    Returns: dict of path -> DataFrame.
    """
    dv = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(run_dv_worker, path): path
            for path in set(paths)
        }
        for fut in as_completed(futures):
            path, df = fut.result()
            dv[path] = df
    return dv


# RAFM funct
thread_count = os.cpu_count()
WORKER_RAFM = Path(__file__).resolve().parent / "rafmul_worker.py"
WORKER_UVSG = Path(__file__).resolve().parent / "uvsg_worker.py"

def run_rafm_worker(run, path):
    # invoke the *same* Python that's running this script
    subprocess.check_call([
        sys.executable,
        str(WORKER_RAFM),
        run,
        path
    ])
    return run, pd.read_pickle(f"rafm_{run}.pkl")


def run_uvsg_worker(run, path):
    # invoke the *same* Python that's running this script
    subprocess.check_call([
        sys.executable,
        str(WORKER_UVSG),
        run,
        path
    ])
    return run, pd.read_pickle(f"uvsg_{run}.pkl")


def build_rafm_subprocess(filters, max_workers = thread_count - 1):
    rafm = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(run_rafm_worker, run, params['path_rafm']): run
            for run, params in filters.items()
        }
        for fut in as_completed(futures):
            run, df = fut.result()
            df.rename(columns= {'RV_AV_IF': 'rv_av_if'}, inplace= True)
            rafm[run] = df
    return rafm


def build_uvsg_subprocess(filters, max_workers = thread_count - 1):
    uvsg = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(run_uvsg_worker, run, params['path_uvsg']): run
            for run, params in filters.items()
        }
        for fut in as_completed(futures):
            run, df = fut.result()
            uvsg[run] = df
    return uvsg

# Table funct
def filter_goc_by_lob(df, lob):
    """
    Filters df for rows whose 'goc' contains the substring _<lob>_ (e.g. '_L_').
    If lob is empty or None, returns df unchanged.
    """
    if not lob:
        # No filtering needed
        return df
    pat = f"{lob.upper()}_"
    mask = df['goc'].str.contains(pat, case=False, na=False)
    return df[mask]



################################ DV PROCESSING ################################

ulfilter = input_sheet.ulfilter
thread_count = os.cpu_count()
WORKER = Path(__file__).resolve().parent / "dv_worker.py"

all_dv_paths = [params['path_dv'] for params in ulfilter.values()]

if len(set(all_dv_paths)) == 1:
    dv_cache = load_dv_excels(ulfilter)

    # For any run:
    for run_name, params in ulfilter.items():
        dv_runs = {run: dv_cache[params['path_dv']] for run, params in ulfilter.items()}
else:
    # 2. Call the loader (deduplicates inside function)
    dv_cache = build_dv_subprocess(all_dv_paths, thread_count - 1)

    # 3. Now for each run, get the loaded DataFrame by its path
    for run_name, params in ulfilter.items():
        dv_runs = {run: dv_cache[params['path_dv']] for run, params in ulfilter.items()}

for run_name in dv_runs:
    df = dv_runs[run_name]
    if 'sum_assur' in df.columns:
        df = df.drop(columns = 'sum_assur')
    
    filtered_df(df, ulfilter, run_name)
    
cleaned_df = build_cleaned_runs(filtered_runs, usdidr, ulfilter)

################################ DV PROCESSING ################################

# SPACER # 
# sys.exit()


################################ RAFM & USVG PROCESSING ################################

SHEET_NAME = ['extraction_IDR', 'extraction_USD']

rafm_runs = build_rafm_subprocess(ulfilter)
uvsg_runs = build_uvsg_subprocess(ulfilter)

################################ RAFM & USVG PROCESSING ################################


# SPACER # 
# sys.exit()


################################ TABLE DF ################################

table_dfs = {}

for run_name in ulfilter:
    dv_df   = cleaned_df[run_name]
    rafm_df = rafm_runs[run_name]
    uvsg_df = uvsg_runs[run_name]
    
    xdv = pd.concat([rafm_df, uvsg_df])
    
    merged  = pd.merge(dv_df, xdv, on="goc", how="outer")
    merged.fillna(0, inplace = True)
    merged['diff policies'] = merged['pol_num'] - merged['pol_b']
    merged['diff fund'] = merged['total_fund'] - merged['rv_av_if']
    
    table_dfs[run_name] = merged

################################ TABLE DF ################################


# SPACER # 
# sys.exit()


################################ TABLE 1 ################################

table1_df = {}

for run_name in ulfilter:
    table_df = table_dfs[run_name]
    table1_df[run_name] = table_df[~table_df['goc'].str.contains("GS", case=False, na=False)]
    
################################ TABLE 1 ################################

# SPACER # 
# sys.exit()


################################ TABLE 2 ################################

table2_df = {}

for run_name in ulfilter:
    table_df            = table_dfs[run_name]
    table_df            = table_df[table_df['goc'].str.contains("AG_IDR_SH", case=False, na=False)]
    table2_df[run_name] = table_df

################################ TABLE 2 ################################


# SPACER # 
# sys.exit()


################################ TABLE 3 ################################

table3_df = {}

for run_name in ulfilter:
    table_df            = table_dfs[run_name]
    table_df            = table_df[table_df['goc'].str.contains("GS", case=False, na=False)]
    table3_df[run_name] = table_df

################################ TABLE 3 ################################


# SPACER # 
# sys.exit()


################################ TABLE 1 SUMMARY ################################

table1_sum = {}

for run_name in ulfilter:
    
    table1 = table1_df[run_name]
    container_dict = {}
    
    table1_sum[run_name] = container_dict
    for param in table1:
        if param == 'goc':
            continue
        else:
            container_dict[param] = table1[param].sum().item()

################################ TABLE 1 SUMMARY ################################


# SPACER # 
# sys.exit()


################################ TABLE 2 SUMMARY ################################

table2_sum = {}

for run_name in ulfilter:
    
    table2 = table2_df[run_name]
    container_dict = {}
    
    table2_sum[run_name] = container_dict
    for param in table2:
        if param == 'goc':
            continue
        else:
            container_dict[param] = table2[param].sum().item()

################################ TABLE 2 SUMMARY ################################


# SPACER # 
# sys.exit()



################################ TABLE 3 SUMMARY ################################

table3_sum = {}

for run_name in ulfilter:
    
    table3 = table3_df[run_name]
    container_dict = {}
    
    table3_sum[run_name] = container_dict
    for param in table3:
        if param == 'goc':
            continue
        else:
            container_dict[param] = table3[param].sum().item()

################################ TABLE 3 SUMMARY ################################


# SPACER # 
# sys.exit()


################################ TABLE SUMMARY ################################

row1_summary    = {}
row2_summary    = {}
summary_dict    = {}
summary_lst     = [table1_sum]


for run_name in ulfilter:
    rdv     = cleaned_df[run_name]
    rrafm   = rafm_runs[run_name]
    ruvsg   = uvsg_runs[run_name]
    xdv     = pd.concat([rrafm, ruvsg])
    container_dict = {}
    
    row1_summary[run_name] = container_dict
    
    container_dict['pol_num']       = rdv['pol_num'].sum().item()
    container_dict['total_fund']    = rdv['total_fund'].sum().item()
    container_dict['pol_b']         = xdv['pol_b'].sum().item()
    container_dict['rv_av_if']      = xdv['rv_av_if'].sum().item()
    container_dict['diff policies'] = container_dict['pol_num'] - container_dict['pol_b']
    container_dict['diff value']    = container_dict['total_fund'] - container_dict['rv_av_if']

    # Extract all parameter dicts for this run_name from each table
    params_list = [tbl[run_name] for tbl in summary_lst]
    
    # Find all keys (parameters) in the nested dicts
    keys = params_list[0].keys()
    
    # Sum each parameter across all tables
    row2_summary[run_name] = {
        k: sum(d[k] for d in params_list) for k in keys
    }

for run_name in ulfilter:
    row1 = row1_summary[run_name]
    row2 = row2_summary[run_name]
    
    all_keys = set(row1) | set(row2)
    row1_full = {k: row1.get(k, None) for k in all_keys}
    row2_full = {k: row2.get(k, None) for k in all_keys}

    df = pd.DataFrame([row1_full, row2_full])
    
    diff_row = df.iloc[0] - df.iloc[1]

    # Append the diff row
    df = pd.concat([df, diff_row.to_frame().T], ignore_index=True)

    # Name your rows for clarity
    df.index = ['row1_summary', 'row2_summary', 'diff']

    desired_order = ['pol_num', 'total_fund', 'pol_b', 'rv_av_if', 'diff policies', 'diff value']
    df = df.reindex(columns=desired_order)

    summary_dict[run_name] = df

################################ TABLE SUMMARY ################################


# SPACER # 
# sys.exit()


################################ CONTROL SUMMARY ################################

ctrlsum_dict = {}

for run_name in ulfilter:
    dct = {
        'total_dv'    : row1_summary[run_name]['pol_num'],
        'ulshpi_dv'   : table1_sum[run_name]['pol_num'],
        'tasbih_dv'   : table2_sum[run_name]['pol_num'],
        'GS_dv'       : table3_sum[run_name]['pol_num'],
        
        'total_rafm'  : row1_summary[run_name]['pol_b'],
        'ulshpi_rafm' : table1_sum[run_name]['pol_b'],
        'tasbih_rafm' : table2_sum[run_name]['pol_b'],
        'GS_rafm'     : table3_sum[run_name]['pol_b'],
        
        'diff_total' : row1_summary[run_name]['diff policies'],
        'diff_ulshpi': table1_sum[run_name]['diff policies'],
        'diff_tasbih': table2_sum[run_name]['diff policies'],
        'diff_GS'    : table3_sum[run_name]['diff policies'],
        'empty'      : "",
        'usdidr'     : usdidr[run_name]
    }
    
    ctrlsum_dict[run_name] = dct

################################ CONTROL SUMMARY ################################

# for run_name in ulfilter:
#     print(run_name)
#     print(ctrlsum_dict[run_name])

# SPACER # 
# sys.exit()

end_time = time.time()
# elapsed_time(start_time, end_time, 'UL processing')
