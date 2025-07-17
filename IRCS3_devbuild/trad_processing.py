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


def filtered_df(df, filter_dict):
    goc_upper = df['goc'].astype(str).str.upper()
    filtered_runs = {}
    usdidr        = {}

    for run_name, params in filter_dict.items():
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

    return filtered_runs, usdidr


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



# RAFM funct
thread_count = os.cpu_count()
WORKER = Path(__file__).resolve().parent / "rafmtrad_worker.py"

def run_rafm_worker(run, path):
    # invoke the *same* Python that's running this script
    subprocess.check_call([
        sys.executable,
        str(WORKER),
        run,
        path
    ])
    return run, pd.read_pickle(f"rafm_{run}.pkl")


def build_rafm_subprocess(filters, max_workers = thread_count - 1):
    rafm = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(run_rafm_worker, run, params['path_rafm']): run
            for run, params in filters.items()
        }
        for fut in as_completed(futures):
            run, df = fut.result()
            rafm[run] = df
    return rafm



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
dv_trad = pd.read_csv(input_sheet.dv_aztrad_csv, sep= ';', decimal= '.')
tradfilter = input_sheet.tradfilter

# DROP REDUNDANCY IN DATA FRAME
cols_to_drop = (['product_group', 'pre_ann','loan_sa'] 
                + [c for c in dv_trad.columns if c.startswith('Unnamed')])
dv_trad = dv_trad.drop(columns= cols_to_drop)

# DATA FILTERING
filtered_runs, usdidr = filtered_df(dv_trad, tradfilter)

# DATA PROCESSING
cleaned_runs = build_cleaned_runs(filtered_runs, usdidr, tradfilter)

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
    dv_df   = cleaned_runs[run_name]
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


################################ TABLE SUMMARY ################################

row1_summary    = {}
row2_summary    = {}
summary_dict    = {}
summary_lst     = [table1_sum, table2_sum, table3_sum, table4_sum]


for run_name in tradfilter:
    rdv     = cleaned_runs[run_name]
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
        
        'total_rafm' : row1_summary[run_name]['pol_b'],
        'lbtpn_rafm' : table1_sum[run_name]['pol_b'] + table2_sum[run_name]['pol_b'],
        'xYRT_rafm'  : table3_sum[run_name]['pol_b'],
        'YRT_rafm'   : table4_sum[run_name]['pol_b'],
        
        'diff_total' : row1_summary[run_name]['diff policies'],
        'diff_lbtpn' : table1_sum[run_name]['diff policies'] + table2_sum[run_name]['diff policies'],
        'diff_xYRT'  : table3_sum[run_name]['diff policies'],
        'diff_YRT'   : table4_sum[run_name]['diff policies'],
        'empty'      : "",
        'usdidr'     : usdidr[run_name]
    }
    
    ctrlsum_dict[run_name] = dct

################################ CONTROL SUMMARY ################################



# SPACER # 
# sys.exit()

end_time = time.time()
# elapsed_time(start_time, end_time, 'trad processing')