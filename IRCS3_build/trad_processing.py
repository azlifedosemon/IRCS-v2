import pandas as pd
import re
import time
import subprocess
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from IRCS3_input import load_inputs
inputs = load_inputs()
option_channel = inputs['option_channel']
start_time = time.time()


# DV funct
def elapsed_time(start,end,script):
    if round((end - start),0) > 60:
        print(f"\n• {script} RUNTIME: {round((end - start) / 60, 2)} minutes", end='')
    elif (end - start) < 1:
        print(f"\n• {script} RUNTIME: {round((end - start) * 1000, 2)} ms", end= '')
    else:
        print(f"\n• {script} RUNTIME: {round((end - start), 2)} second", end= '')


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
            if x in option_channel:
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
        df['sum_assd'] = pd.to_numeric(
            df['sum_assd'].astype(str)
                .str.replace(',', '.', regex=False),
            errors='coerce'
        )
        
        df = df.groupby(['goc'], as_index= False).sum(numeric_only = True)
        
        usd_mask = df['goc'].str.contains('USD',case = False,na = False)
        df.loc[usd_mask, 'sum_assd'] = df.loc[usd_mask, 'sum_assd'] * usdidr[run_name]
        
        # 4) Store back into the new dict
        cleaned_runs[run_name] = df

    return cleaned_runs


def load_dv_excels(tradfilter):
    """
    Untuk dict run config TRAD, kembalikan dict: path_dv -> DataFrame,
    dan pastikan tiap file hanya dibaca satu kali.
    """
    cache = {}
    for run_params in tradfilter.values():
        path = run_params['path_dv']
        if path not in cache:
            df = pd.read_excel(path, engine = 'openpyxl')
            cols_to_drop = (
                ['product_group', 'pre_ann', 'loan_sa'] +
                [c for c in df.columns if str(c).startswith('Unnamed')]
            )
            df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
            cache[path] = df
    return cache

def run_dv_worker(path):
    out_pkl = str(Path.cwd() / f"dv_{Path(path).stem}.pkl")
    df = pd.read_csv(path)
    cols_to_drop = (
        ['product_group', 'pre_ann', 'loan_sa'] +
        [c for c in df.columns if str(c).startswith('Unnamed')]
    )
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    df.to_pickle(out_pkl)
    return path, df

def build_dv_subprocess(paths, max_workers):
    dv = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(run_dv_worker, path): path for path in set(paths)}
        for fut in as_completed(futures):
            path, df = fut.result()
            dv[path] = df
    return dv


# RAFM funct
thread_count = os.cpu_count()
WORKER = Path(__file__).resolve().parent / "rafmtrad_worker.py"

def run_rafm_worker(input_path, output_path, run_id):
    try:
        subprocess.run([
            sys.executable,
            "d:\\Run Control 3\\IRCS3_build\\rafm_worker.py",
            input_path,
            output_path
        ], check=True)

        if not os.path.exists(output_path):
            print(f"❌ File not found: {output_path}")
            return None

        df = pd.read_pickle(output_path)
        return (run_id, df)

    except subprocess.CalledProcessError as e:
        print(f"❌ Error in RAFM worker for run {run_id}: {e}")
        return None



def build_rafm_subprocess(tradfilter, output_dir):

    results = []
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor() as executor:
        futures = []
        for run_name, params in tradfilter.items():
            input_path = params.get("RAFM FILE PATH")
            output_path = output_dir / f"rafm_{run_name}.pkl"
            futures.append(executor.submit(run_rafm_worker, input_path, output_path, run_name))

        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"❌ Error in RAFM subprocess for {run_name}: {e}")

    return dict(results)




# Table funct
def filter_goc_by_lob(df, lob):
    """
    Filters df for rows where 'goc':
      • starts with '<LOB>_'   OR  
      • contains '_<LOB>_' 
    e.g. lob='L' matches 'L_...' and '..._L_...'
    If lob is empty or None, returns df unchanged.
    """
    if not lob:
        # No filtering needed
        return df
    pat = fr"(^|_){lob.upper()}_"
    mask = df['goc'].str.contains(pat, case=False, na=False)
    return df[mask]




################################ DV PROCESSING ################################
tradfilter = inputs['tradfilter']
thread_count = os.cpu_count()
WORKER = Path(__file__).resolve().parent / "dv_worker.py"

all_dv_paths = [params['path_dv'] for params in tradfilter.values()]

if len(set(all_dv_paths)) == 1:
    dv_cache = load_dv_excels(tradfilter)
else:
    dv_cache = build_dv_subprocess(all_dv_paths, thread_count - 1)

dv_runs = {run: dv_cache[params['path_dv']] for run, params in tradfilter.items()}

filtered_runs = {}
usdidr = {}

for run_name, df in dv_runs.items():
    if 'sum_assur' in df.columns:
        df = df.drop(columns='sum_assur')

    filtered_df(df, tradfilter, run_name)

cleaned_df = build_cleaned_runs(filtered_runs, usdidr, tradfilter)


################################ DV PROCESSING ################################


# SPACER # 
# sys.exit()


################################ RAFM PROCESSING ################################

SHEET_NAME = ['extraction_IDR', 'extraction_USD']

rafm_runs = build_rafm_subprocess(tradfilter)

################################ RAFM PROCESSING ################################


# SPACER # 
# sys.exit()


################################ TABLE DF ################################

table_dfs = {}

for run_name in tradfilter:
    dv_df   = cleaned_df[run_name]
    rafm_df = rafm_runs[run_name]
    
    merged  = pd.merge(dv_df, rafm_df, on="goc", how="outer")
    merged.fillna(0, inplace = True)
    merged['diff policies'] = merged['pol_num'] - merged['pol_b']
    merged['diff sa'] = merged['sum_assd'] - merged['cov_units']
    
    table_dfs[run_name] = merged

################################ TABLE DF ################################


# SPACER # 
# sys.exit()


################################ TABLE 1 ################################

table1_df = {}

for run_name in tradfilter:
    table_df            = filter_goc_by_lob(table_dfs[run_name], 'L')
    table_df            = table_df[~table_df['goc'].str.contains("%", case=False, na=False)]
    table1_df[run_name] = table_df
    
################################ TABLE 1 ################################


# SPACER # 
# sys.exit()


################################ TABLE 2 ################################

table2_df = {}

for run_name in tradfilter:
    table_df            = filter_goc_by_lob(table_dfs[run_name], 'L')
    table_df            = table_df[table_df['goc'].str.contains("%", case=False, na=False)]
    table2_df[run_name] = table_df

################################ TABLE 2 ################################


# SPACER # 
# sys.exit()


################################ TABLE 3 ################################

table3_df = {}

for run_name in tradfilter:
    table_df            = filter_goc_by_lob(table_dfs[run_name], 'H')
    table_df            = table_df[~table_df['goc'].str.contains("YR", case=False, na=False)]
    table3_df[run_name] = table_df

################################ TABLE 3 ################################


# SPACER # 
# sys.exit()


################################ TABLE 4 ################################

table4_df = {}

for run_name in tradfilter:
    table_df            = filter_goc_by_lob(table_dfs[run_name], 'H')
    table_df            = table_df[table_df['goc'].str.contains("YR", case=False, na=False)]
    table4_df[run_name] = table_df

################################ TABLE 4 ################################


# SPACER # 
# sys.exit()


################################ TABLE 5 ################################

table5_df = {}

for run_name in tradfilter:
    table_df            = filter_goc_by_lob(table_dfs[run_name], 'C')
    table5_df[run_name] = table_df

################################ TABLE 5 ################################


# SPACER # 
# sys.exit()


################################ TABLE 1 SUMMARY ################################

table1_sum = {}

for run_name in tradfilter:
    
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

for run_name in tradfilter:
    
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

for run_name in tradfilter:
    
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


################################ TABLE 4 SUMMARY ################################

table4_sum = {}

for run_name in tradfilter:
    
    table4 = table4_df[run_name]
    container_dict = {}
    
    table4_sum[run_name] = container_dict
    for param in table4:
        if param == 'goc':
            continue
        else:
            container_dict[param] = table4[param].sum().item()

################################ TABLE 4 SUMMARY ################################


# SPACER # 
# sys.exit()


################################ TABLE 5 SUMMARY ################################

table5_sum = {}

for run_name in tradfilter:
    
    table5 = table5_df[run_name]
    container_dict = {}
    
    table5_sum[run_name] = container_dict
    for param in table5:
        if param == 'goc':
            continue
        else:
            container_dict[param] = table5[param].sum().item()

################################ TABLE 5 SUMMARY ################################


# SPACER # 
# sys.exit()


################################ TABLE SUMMARY ################################

row1_summary    = {}
row2_summary    = {}
summary_dict    = {}
summary_lst     = [table1_sum, table2_sum, table3_sum, table4_sum, table5_sum]


for run_name in tradfilter:
    rdv     = cleaned_df[run_name]
    rrafm   = rafm_runs[run_name]
    container_dict = {}
    
    row1_summary[run_name] = container_dict
    
    container_dict['pol_num']       = rdv['pol_num'].sum().item()
    container_dict['sum_assd']      = rdv['sum_assd'].sum().item()
    container_dict['pol_b']         = rrafm['pol_b'].sum().item()
    container_dict['cov_units']     = rrafm['cov_units'].sum().item()
    container_dict['diff policies'] = container_dict['pol_num'] - container_dict['pol_b']
    container_dict['diff sa']       = container_dict['sum_assd'] - container_dict['cov_units']

    # Extract all parameter dicts for this run_name from each table
    params_list = [tbl[run_name] for tbl in summary_lst]
    
    # Find all keys (parameters) in the nested dicts
    keys = params_list[0].keys()
    
    # Sum each parameter across all tables
    row2_summary[run_name] = {
        k: sum(d[k] for d in params_list) for k in keys
    }

for run_name in tradfilter:
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

    desired_order = ['pol_num', 'sum_assd', 'pol_b', 'cov_units', 'diff policies', 'diff sa']
    df = df.reindex(columns=desired_order)

    summary_dict[run_name] = df



################################ TABLE SUMMARY ################################

# SPACER # 
# sys.exit()


################################ CONTROL SUMMARY ################################

ctrlsum_dict = {}

for run_name in tradfilter:
    dct = {
        'total_dv': row1_summary[run_name]['pol_num'],
        'lbtpn_dv': table1_sum[run_name]['pol_num'] + table2_sum[run_name]['pol_num'],
        'xYRT_dv' : table3_sum[run_name]['pol_num'],
        'YRT_dv'  : table4_sum[run_name]['pol_num'],
        'c_dv'    : table5_sum[run_name]['pol_num'],
        
        'total_rafm' : row1_summary[run_name]['pol_b'],
        'lbtpn_rafm' : table1_sum[run_name]['pol_b'] + table2_sum[run_name]['pol_b'],
        'xYRT_rafm'  : table3_sum[run_name]['pol_b'],
        'YRT_rafm'   : table4_sum[run_name]['pol_b'],
        'c_rafm'     : table5_sum[run_name]['pol_b'],
        
        'diff_total' : row1_summary[run_name]['diff policies'],
        'diff_lbtpn' : table1_sum[run_name]['diff policies'] + table2_sum[run_name]['diff policies'],
        'diff_xYRT'  : table3_sum[run_name]['diff policies'],
        'diff_YRT'   : table4_sum[run_name]['diff policies'],
        'diff_c'     : table5_sum[run_name]['diff policies'],
        'empty'      : "",
        'usdidr'     : usdidr[run_name]
    }
    
    ctrlsum_dict[run_name] = dct

################################ CONTROL SUMMARY ################################



# SPACER # 
# sys.exit()

end_time = time.time()
# elapsed_time(start_time, end_time, 'trad processing')