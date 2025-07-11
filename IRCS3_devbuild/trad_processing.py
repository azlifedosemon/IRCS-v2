import pandas as pd
import re
import IRCS3_input as input_sheet
import time

start_time = time.time()

dv_trad = pd.read_csv(input_sheet.dv_aztrad_csv, sep= ';', decimal= '.')

# DATA FILTERING
tradfilter = input_sheet.tradfilter
filtered_runs = {}
usdidr        = {}

def make_pattern(tokens):
    return '|'.join(re.escape(tok) for tok in tokens) if tokens else None

for run_name, params in tradfilter.items():
    # 1) Start with all rows
    mask = pd.Series(True, index=dv_trad.index)
    
    # 2) Grab the USD-to-IDR rate for this run (first element of the list)
    rate = params.get('USDIDR')
    if rate is not None:
        usdidr[run_name] = rate
    
    # 3) Apply “only_” filters (include if ANY token matches)
    for key in ['only_kite', 'only_currency', 'only_portfolio', 'only_cohort']:
        tokens = params.get(key) or []
        pattern = make_pattern(tokens)
        if pattern:
            mask &= dv_trad['goc'].str.contains(pattern, case=False, na=False)

    # 4) Apply “exclude_” filters (drop if ANY token matches)
    for key in ['exclude_kite', 'exclude_currency', 'exclude_portfolio', 'exclude_cohort']:
        tokens = params.get(key) or []
        pattern = make_pattern(tokens)
        if pattern:
            mask &= ~dv_trad['goc'].str.contains(pattern, case=False, na=False)

    # 5) Store the filtered result
    filtered_runs[run_name] = dv_trad[mask].copy()


end_time = time.time()