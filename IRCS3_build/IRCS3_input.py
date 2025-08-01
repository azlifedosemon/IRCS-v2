import pandas as pd
from pathlib import Path

# === SET INPUT SHEET PATH SECARA OTOMATIS (tidak hardcoded)
INPUT_SHEET_PATH = Path(__file__).parent / "Input Sheet_IRCS3.xlsx"

def to_list(val):
    if pd.isna(val):
        return []
    return [str(v).strip() for v in str(val).split(',') if str(v).strip()]

def filter_processing(df, name):
    df.columns = df.columns.str.strip()
    df = df.dropna(how='all')

    if df.empty:
        print(f"Warning: Sheet {name} is empty.")
        return []

    filters = []
    for _, row in df.iterrows():
        row = row.fillna('')
        only_channel     = to_list(row.get('only_channel'))
        exclude_channel  = to_list(row.get('exclude_channel'))
        only_portfolio   = to_list(row.get('only_portfolio'))
        exclude_portfolio= to_list(row.get('exclude_portfolio'))
        only_cohort      = to_list(row.get('only_cohort'))
        exclude_cohort   = to_list(row.get('exclude_cohort'))
        only_currency    = to_list(row.get('only_currency'))
        exclude_currency = to_list(row.get('exclude_currency'))
        goc              = to_list(row.get('goc'))
        usd_rate         = pd.to_numeric(str(row.get('USDIDR', '')).strip(), errors='coerce')

        if only_channel and exclude_channel:
            raise ValueError("Cannot have both 'only_channel' and 'exclude_channel'")
        if only_portfolio and exclude_portfolio:
            raise ValueError("Cannot have both 'only_portfolio' and 'exclude_portfolio'")

        filters.append({
            "only_channel": only_channel,
            "exclude_channel": exclude_channel,
            "only_portfolio": only_portfolio,
            "exclude_portfolio": exclude_portfolio,
            "only_cohort": only_cohort,
            "exclude_cohort": exclude_cohort,
            "only_currency": only_currency,
            "exclude_currency": exclude_currency,
            "goc": goc,
            "usd_rate": usd_rate,
            "name": name
        })

    return filters

# === BACA SEMUA SHEET
excel = pd.read_excel(INPUT_SHEET_PATH, sheet_name=None)

FILTER_TRAD = excel.get('FILTER_TRAD', pd.DataFrame())
FILTER_UL   = excel.get('FILTER_UL', pd.DataFrame())
PATH_MAP    = excel.get('File Path', pd.DataFrame())

# === STRIP SEMUA KOLOM DI AWAL
for k in excel:
    if isinstance(excel[k], pd.DataFrame):
        excel[k].columns = excel[k].columns.str.strip()

# === BUAT FILTER
tradfilter = filter_processing(FILTER_TRAD, 'FILTER_TRAD')
ulfilter   = filter_processing(FILTER_UL, 'FILTER_UL')

# === GET FILE PATH DYNAMIC
def get_output_path(key, ref_key, path_map_df):
    path_map_df.columns = path_map_df.columns.str.strip()
    mydict = dict(zip(path_map_df['KEY'], path_map_df['PATH']))
    if key in mydict:
        base_path = Path(mydict.get(ref_key, '')).resolve().parent
        return str(base_path / mydict[key])
    return None

excel_output_trad = get_output_path('Output Trad', 'DV_AZTRAD', PATH_MAP)
excel_output_ul   = get_output_path('Output UL', 'DV_UL', PATH_MAP)

# === GET FILE PATHS
dv_aztrad_csv = PATH_MAP[PATH_MAP['KEY'] == 'DV_AZTRAD']['PATH'].values[0]
rafmtrad_path = PATH_MAP[PATH_MAP['KEY'] == 'RAFM_TRAD']['PATH'].values[0]
dv_ul_csv     = PATH_MAP[PATH_MAP['KEY'] == 'DV_UL']['PATH'].values[0]
rafm_ul_path  = PATH_MAP[PATH_MAP['KEY'] == 'RAFM_UL']['PATH'].values[0]
uvsg_ul_path  = PATH_MAP[PATH_MAP['KEY'] == 'UVSG_UL']['PATH'].values[0]

# === DEBUG OPTIONAL
# print(tradfilter)
# print(ulfilter)
# print(excel_output_trad, excel_output_ul)
