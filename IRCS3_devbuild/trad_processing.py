import pandas as pd
import re
import IRCS3_input as input_sheet
import time

start_time = time.time()

def filtered_df(df, filter_dict):
    
    # 1) Preprocess: uppercase GOC once
    goc_upper = df['goc'].str.upper()

    # 2) Define include/exclude keys
    include_keys = ('only_kite', 'only_currency', 'only_portfolio', 'only_cohort', 'only_period')
    exclude_keys = ('exclude_kite', 'exclude_currency', 'exclude_portfolio', 'exclude_cohort', 'exclude_period')

    # 3) Collect all unique patterns across all runs
    all_patterns = set()
    for params in filter_dict.values():
        for key in include_keys + exclude_keys:
            tokens = params.get(key) or []
            if tokens:
                # build regex alternation, uppercase tokens too
                pat = '|'.join(re.escape(tok.upper()) for tok in tokens)
                all_patterns.add(pat)

    # 4) Precompute a Boolean mask for each pattern
    pattern_masks = {
        pat: goc_upper.str.contains(pat, na=False)
        for pat in all_patterns
    }

    # 5) Apply filters per run using only cached masks
    filtered_runs = {}
    usdidr        = {}

    for run_name, params in filter_dict.items():
        # start with all‐True mask
        mask = pd.Series(True, index=dv_trad.index)
        
        # grab USDIDR rate
        rate = params.get('USDIDR')
        if rate is not None:
            usdidr[run_name] = rate
        
        # apply “only_” filters
        for key in include_keys:
            tokens = params.get(key) or []
            if tokens:
                pat = '|'.join(re.escape(tok.upper()) for tok in tokens)
                mask &= pattern_masks[pat]
        
        # apply “exclude_” filters
        for key in exclude_keys:
            tokens = params.get(key) or []
            if tokens:
                pat = '|'.join(re.escape(tok.upper()) for tok in tokens)
                mask &= ~pattern_masks[pat]
        
        # store the filtered DataFrame (no extra copy)
        filtered_runs[run_name] = dv_trad.loc[mask]
    
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
                       .str.replace(",", ".", regex=False),
            errors='coerce'
        )
        df['sum_assd'] = pd.to_numeric(
            df['sum_assd'].astype(str)
                .str.replace(",", ".", regex=False),
            errors='coerce'
        )
        
        df = df.groupby(['goc'], as_index= False).sum(numeric_only = True)
        
        usd_mask = df["goc"].str.contains("USD",case = False,na = False)
        df.loc[usd_mask, 'sum_assd'] = df.loc[usd_mask, 'sum_assd'] * usdidr[run_name]
        
        # 4) Store back into the new dict
        cleaned_runs[run_name] = df

    return cleaned_runs


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



################################ RAFM PROCESSING ################################














################################ RAFM PROCESSING ################################




end_time = time.time()