import pandas as pd
import re
import sys
from pathlib import Path

# === SET INPUT SHEET PATH SECARA OTOMATIS (tidak hardcoded)
INPUT_SHEET_PATH = Path(__file__).parent / "Input Sheet_IRCS3.xlsx"



def to_list(cell):
    splitter = re.compile(r'[,\;/\\\|\s]+')
    s = str(cell).strip()
    if not s:
        return []
    parts = splitter.split(s)
    return list(dict.fromkeys(tok.strip() for tok in parts if tok.strip()))


def normalize_cohort_token(tok):
    s = str(tok).strip()
    m = re.fullmatch(r'(\d+)(?:\.0+)?', s)
    return m.group(1) if m else s


def get_value(key, mydict):
    return mydict.get(key)


def get_local_folder(filepath):
    return str(Path(filepath).parent)


def get_output_path(filekey, refkey, path_map):
    filename = path_map.get(filekey)
    ref_path = path_map.get(refkey)
    if filename and ref_path:
        return str(Path(get_local_folder(ref_path)) / f"{filename}.xlsx")
    return None


def filter_processing(filter_df, sheetname):
    filter_df.columns = filter_df.columns.map(str)
    filter_df.columns = filter_df.columns.str.strip()

    for col in filter_df.columns:
        if col == 'run_name':
            continue
        elif col in ['path_dv', 'path_rafm', 'path_uvsg']:
            filter_df[col] = filter_df[col].astype(str).str.strip().replace('', pd.NA)
        elif col.strip().upper() == 'USDIDR':
            filter_df[col] = pd.to_numeric(filter_df[col].astype(str).str.strip().replace('', pd.NA), errors='coerce')
        else:
            lst = filter_df[col].apply(to_list)
            if col in ('only_cohort', 'exclude_cohort'):
                lst = lst.apply(lambda toks: [normalize_cohort_token(tok) for tok in toks])
            filter_df[col] = lst

    filter_df = filter_df.fillna('')

    # Pastikan semua filter bertipe list
    for col in filter_df.columns:
        if col.startswith('only_') or col.startswith('exclude_'):
            filter_df[col] = filter_df[col].apply(lambda x: x if isinstance(x, list) else [])

    # Validasi kolom wajib
    missing_print = []
    if 'path_rafm' in filter_df:
        missing_path_rafm = filter_df[filter_df['path_rafm'] == '']['run_name'].tolist()
        if missing_path_rafm:
            missing_print.append(f"ERROR PATH: Fill 'path_rafm' for {', '.join(missing_path_rafm)} in {sheetname}")

    if 'USDIDR' in filter_df:
        missing_rate = filter_df[filter_df['USDIDR'] == '']['run_name'].tolist()
        if missing_rate:
            missing_print.append(f"ERROR RATE: Fill 'USDIDR' for {', '.join(missing_rate)} in {sheetname}")

    if missing_print:
        for msg in missing_print:
            print(msg)
        sys.exit(1)

    filters = filter_df.set_index('run_name').to_dict(orient='index')

    # Clash checker (abaikan kalau salah satu kosong)
    run_clashes = {}
    for run_name, params in filters.items():
        for key, only_list in params.items():
            if not key.startswith('only_'):
                continue
            category = key[len('only_'):]
            excl_list = params.get(f'exclude_{category}', [])
            only_list = only_list or []
            excl_list = excl_list or []
            if not isinstance(only_list, list): only_list = []
            if not isinstance(excl_list, list): excl_list = []
            common = set(only_list) & set(excl_list)
            if common:
                run_clashes.setdefault(run_name, {}).setdefault(category, set()).update(common)

    if run_clashes:
        print(f"ERROR CLASH in {sheetname}:")
        for run_name, cat_map in run_clashes.items():
            details = "; ".join(f"{cat} → {', '.join(sorted(vals))}" for cat, vals in cat_map.items())
            print(f"• {run_name}: {details}")
        sys.exit(1)

    return filters




def load_inputs(input_sheet_path):
    input_sheet = pd.ExcelFile(input_sheet_path, engine='openpyxl')
    path_df = pd.read_excel(input_sheet, 'INPUT_SETTING')
    variable_df = pd.read_excel(input_sheet, 'VARIABLE_DEF')

    path_map = dict(zip(path_df['Category'], path_df['Path']))
    var_map = dict(zip(variable_df['Variable Name'], variable_df['Options']))

    filter_trad_raw = pd.read_excel(input_sheet, 'FILTER_TRAD')
    filter_ul_raw = pd.read_excel(input_sheet, 'FILTER_UL')

    tradfilter = filter_processing(filter_trad_raw, 'FILTER_TRAD')
    ulfilter = filter_processing(filter_ul_raw, 'FILTER_UL')

    valuation_month = str(get_value('Valuation Month', path_map)) + 'M'
    valuation_year = get_value('Valuation Year', path_map)
    valuation_rate = get_value('FX Rate Valdate', path_map)
    product_model = get_value('Product Model', path_map)

    excel_output_trad = get_output_path('Output Trad', 'Output Path Trad', path_map)
    excel_output_ul = get_output_path('Output UL', 'Output Path UL', path_map)

    # Variabel
    option_channel = get_value('channel', var_map).split(',')
    option_currency = get_value('currency', var_map).split(',')
    option_portfolio = get_value('portfolio', var_map).split(',')
    option_period = get_value('period', var_map).split(',')

    return {
        'tradfilter': tradfilter,
        'ulfilter': ulfilter,
        'valuation_month': valuation_month,
        'valuation_year': valuation_year,
        'valuation_rate': valuation_rate,
        'product_model': product_model,
        'excel_output_trad': excel_output_trad,
        'excel_output_ul': excel_output_ul,
        'option_channel': option_channel,
        'option_currency': option_currency,
        'option_portfolio': option_portfolio,
        'option_period': option_period
    }
