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
            for tok in tokens:
                # Use regex boundaries: match "_TOKEN_" at start, middle, or end
                pat = fr'(?:^|_){re.escape(tok.upper())}(?:_|$)'
                mask &= ~goc_upper.str.contains(pat, na=False, regex=True)

        # Only filters (if set)
        for key in ('only_channel', 'only_currency', 'only_portfolio', 'only_cohort', 'only_period'):
            tokens = tokens_list(params.get(key))
            for tok in tokens:
                pat = fr'(?:^|_){re.escape(tok.upper())}(?:_|$)'
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
WORKER = Path(__file__).resolve().parent / "rafm_worker.py"

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



end_time = time.time()
# elapsed_time(start_time, end_time, 'ul processing')