import pandas as pd
import re

dv_trad = pd.read_csv("D:\IRCS\Control 3\input\DV_AZTRAD_Stat_0625_v1.csv", sep = ",")
dv_trad
kecuali_tahun = input()
tahun_tertentu = input()
produk_tertentu_1 = input()
produk_tertentu_2 = input()
produk_tertentu_3 = input()
kecuali_produk_1 = input()
kecuali_produk_2 = input()
kecuali_produk_3 = input()
usd_rate = 16233
mask = pd.Series(True, index=dv_trad.index)

if kecuali_tahun and kecuali_tahun != '-':
    mask &= ~dv_trad['goc'].str.contains(kecuali_tahun, case=False, na=False)

if tahun_tertentu and tahun_tertentu != '-':
    mask &= dv_trad['goc'].str.contains(tahun_tertentu, case=False, na=False)

produk_mask = pd.Series(False, index=dv_trad.index)

if produk_tertentu_1 and produk_tertentu_1 != '-':
    produk_mask |= dv_trad['goc'].apply(lambda x: produk_tertentu_1.lower() in [t.lower() for t in str(x).split('_')])

if produk_tertentu_2 and produk_tertentu_2 != '-':
    produk_mask |= dv_trad['goc'].apply(lambda x: produk_tertentu_2.lower() in [t.lower() for t in str(x).split('_')])

if produk_tertentu_3 and produk_tertentu_3 != '-':
    produk_mask |= dv_trad['goc'].apply(lambda x: produk_tertentu_3.lower() in [t.lower() for t in str(x).split('_')])

if (
    (produk_tertentu_1 and produk_tertentu_1 != '-') or
    (produk_tertentu_2 and produk_tertentu_2 != '-') or
    (produk_tertentu_3 and produk_tertentu_3 != '-')
):
    mask &= produk_mask

if kecuali_produk_1 and kecuali_produk_1 != '-':
    mask &= ~dv_trad['goc'].apply(lambda x: kecuali_produk_1.lower() in [t.lower() for t in str(x).split('_')])

if kecuali_produk_2 and kecuali_produk_2 != '-':
    mask &= ~dv_trad['goc'].apply(lambda x: kecuali_produk_2.lower() in [t.lower() for t in str(x).split('_')])

if kecuali_produk_3 and kecuali_produk_3 != '-':
    mask &= ~dv_trad['goc'].apply(lambda x: kecuali_produk_3.lower() in [t.lower() for t in str(x).split('_')])

dv_trad_total = dv_trad[mask]

dv_trad_total = dv_trad_total.drop(columns=['product_group','pre_ann','loan_sa'])

def get_sortir(tahun_tertentu):
    def sortir(name):
        if '____' in name:
            double_underscore_parts = name.split('____')
            if len(double_underscore_parts) > 1:
                after_double = double_underscore_parts[-1]
                after_parts = [p for p in after_double.split('_') if p]
                
                year_index_after = -1
                for i, part in enumerate(after_parts):
                    if re.fullmatch(r'\d{4}', part):
                        year_index_after = i
                        break
                
                if tahun_tertentu and 'Q1' in tahun_tertentu.upper():
                    return after_double
                
                if year_index_after == -1:
                    return ''
                return '_'.join(after_parts[:year_index_after + 1])

        parts = [p for p in name.split('_') if p]

        year_index = -1
        for i, part in enumerate(parts):
            if re.fullmatch(r'\d{4}', part):
                year_index = i
                break

        start_index = None
        for i, part in enumerate(parts):
            if part == 'AG':
                start_index = i
                break
        if start_index is None:
            start_index = 2

        if tahun_tertentu and 'Q1' in tahun_tertentu.upper():
            return '_'.join(parts[start_index:])
        
        if year_index == -1:
            return ''
        return '_'.join(parts[start_index:year_index + 1])
    
    return sortir

dv_trad_total['goc'] = dv_trad_total['goc'].apply(get_sortir(tahun_tertentu))

dv_trad_total['goc'] = dv_trad_total['goc'].apply(lambda x: 'H_IDR_NO_2025' if x == 'IDR_NO_2025' else x)

dv_trad_total["pol_num"] = (
    dv_trad_total["pol_num"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

dv_trad_total["pol_num"] = pd.to_numeric(
    dv_trad_total["pol_num"], errors="coerce"
)

dv_trad_total["sum_assd"] = (
    dv_trad_total["sum_assd"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

dv_trad_total["sum_assd"] = pd.to_numeric(
    dv_trad_total["sum_assd"], errors="coerce"
)

dv_trad_total = dv_trad_total.groupby(["goc"],as_index=False).sum(numeric_only=True)
usd_mask = dv_trad_total["goc"].str.contains("USD",case = False,na = False)
dv_trad_total.loc[usd_mask, 'sum_assd'] = dv_trad_total.loc[usd_mask, 'sum_assd'] * usd_rate
dv_trad_total


################################ RAFM PROCESSING ################################
run_rafm_idr = pd.read_excel("D:\IRCS\Control 3\input\Data_Extraction_run4TRAD_Sha.xlsx",sheet_name = 'extraction_IDR',engine = 'openpyxl')
run_rafm_idr = run_rafm_idr[['GOC','period','cov_units','pol_b']]
mask_rafm = run_rafm_idr['period'].astype(str) == '0'
run_rafm_idr = run_rafm_idr[mask_rafm]
run_rafm_idr = run_rafm_idr.drop(columns = ["period"])
run_rafm_usd = pd.read_excel("D:\IRCS\Control 3\input\Data_Extraction_run4TRAD_Sha.xlsx",sheet_name = 'extraction_USD',engine = 'openpyxl')
run_rafm_usd = run_rafm_usd[['GOC','period','cov_units','pol_b']]
mask_rafm = run_rafm_usd['period'].astype(str) == '0'
run_rafm_usd = run_rafm_usd[mask_rafm]
run_rafm_usd = run_rafm_usd.drop(columns = ["period"])
run_rafm = pd.concat([run_rafm_idr,run_rafm_usd])

run_rafm["pol_b"] = (
    run_rafm["pol_b"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

run_rafm["pol_b"] = pd.to_numeric(
    run_rafm["pol_b"], errors="coerce"
)

run_rafm["cov_units"] = (
    run_rafm["cov_units"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

run_rafm["cov_units"] = pd.to_numeric(
    run_rafm["cov_units"], errors="coerce"
)

run_rafm
run_rafm = run_rafm.rename(columns={'GOC':'goc'})
run_rafm

# merging
merged = pd.merge(dv_trad_total, run_rafm, on="goc", how="outer", 
                  suffixes=("_trad_total", "run_rafm"))

merged.fillna(0, inplace=True)
merged['diff policies'] = merged['pol_num'] - merged['pol_b']
merged['diff sa'] = merged['sum_assd'] - merged['cov_units']
merged

def filter_goc_by_code(df, code):
    tokens = [k for k in code.split('_') if k]
    mask = merged['goc'].apply(lambda x: all(token.lower() in str(x).lower() for token in tokens))
    return merged[mask]
code_tabel_total_l = 'l'

tabel_total_l = filter_goc_by_code(merged, code_tabel_total_l)
tabel_total_l = tabel_total_l[~tabel_total_l['goc'].str.contains("%", case=False, na=False)]

tabel_total_l

dv_total_policies = dv_trad_total['pol_num'].sum()
dv_total_sa = dv_trad_total['sum_assd'].sum()
dv_tabe_l_policies = tabel_total_l['pol_num'].sum()
dv_tabe_l_sa = tabel_total_l['sum_assd'].sum()

rafm_total_policies = run_rafm['pol_b'].sum()
rafm_total_sa = run_rafm['cov_units'].sum()
rafm_tabel_l_policies = tabel_total_l['pol_b'].sum()
rafm_tabel_l_sa = tabel_total_l['cov_units'].sum()

diff_policies = dv_total_policies - rafm_total_policies
diff_sa = dv_total_sa - rafm_total_sa
diff_policies_tabel_l = dv_tabe_l_policies - rafm_tabel_l_policies
diff_sa_tabel_l = dv_tabe_l_sa - rafm_tabel_l_sa

summary = pd.DataFrame({
    '': ['Total Trad All from DV', 'Grand Total Summary', 'Check'],
    'DV # of Policies': [dv_total_policies,dv_tabe_l_policies, dv_total_policies-dv_tabe_l_policies],
    'DV SA': [dv_total_sa,dv_tabe_l_sa,dv_total_sa-dv_tabe_l_sa ],
    'RAFM # of Policies': [rafm_total_policies, rafm_tabel_l_policies, rafm_total_policies-rafm_tabel_l_policies],
    'RAFM SA': [rafm_total_sa, rafm_tabel_l_sa,rafm_total_sa-rafm_tabel_l_sa],
    'Diff # of Policies': [diff_policies, diff_policies_tabel_l,diff_policies-diff_policies_tabel_l],
    'Diff SA': [diff_sa,diff_sa_tabel_l,diff_sa-diff_sa_tabel_l]
})
summary
code_tabel_2 = 'CC%'
tabel_2 = filter_goc_by_code(merged, code_tabel_2)
tabel_2
dv_policies_tabel_2 = tabel_2['pol_num'].sum()
dv_sa_tabel_2 = tabel_2['sum_assd'].sum()
rafm_policies_tabel_2 = tabel_2['pol_b'].sum()
rafm_sa_tabel_2 = tabel_2['cov_units'].sum()
diff_policies_tabel_2 = dv_policies_tabel_2-rafm_policies_tabel_2
diff_sa_tabel_2 = dv_sa_tabel_2-rafm_sa_tabel_2

summary_tabel_2 = pd.DataFrame([{
    "DV": dv_policies_tabel_2,
    "DV SA": dv_sa_tabel_2,
    "RAFM Output": rafm_policies_tabel_2, 
    "RAFM SA": rafm_sa_tabel_2,
    'Diff # of Policies':diff_policies_tabel_2,
    'Diff SA': diff_sa_tabel_2
}])

summary_tabel_2
code_tabel_3 = input()
tabel_3 = filter_goc_by_code(merged, code_tabel_3)
tabel_3['goc'] = tabel_3['goc'].apply(lambda x: '_'.join(x.split('_')[0:4]) if x.startswith('H_IDR_NO') else '_'.join(x.split('_')[1:5]))
tabel_3 = tabel_3.groupby(['goc'],as_index=False).sum(numeric_only=True)
tabel_3
dv_policies_tabel_3 = tabel_3['pol_num'].sum()
dv_sa_tabel_3 = tabel_3['sum_assd'].sum()
rafm_policies_tabel_3 = tabel_3['pol_b'].sum()
rafm_sa_tabel_3= tabel_3['cov_units'].sum()
diff_policies_tabel_3 = dv_policies_tabel_3-rafm_policies_tabel_3
diff_sa_tabel_3 = dv_sa_tabel_3-rafm_sa_tabel_3

summary_tabel_3 = pd.DataFrame([{
    "DV": dv_policies_tabel_3,
    "DV SA": dv_sa_tabel_3,
    "RAFM Output": rafm_policies_tabel_3, 
    "RAFM SA": rafm_sa_tabel_3,
    'Diff # of Policies':diff_policies_tabel_3,
    'Diff SA': diff_sa_tabel_3
}])

summary_tabel_3
code_tabel_4 = input()
tabel_4 = filter_goc_by_code(merged, code_tabel_4)
tabel_4['goc'] = tabel_4['goc'].apply(lambda x: '_'.join(x.split('_')[1:5]))
tabel_4 = tabel_4.groupby(['goc'],as_index=False).sum(numeric_only=True)
tabel_4 
dv_policies_tabel_4 = tabel_4['pol_num'].sum()
dv_sa_tabel_4 = tabel_4['sum_assd'].sum()
rafm_policies_tabel_4 = tabel_4['pol_b'].sum()
rafm_sa_tabel_4= tabel_4['cov_units'].sum()
diff_policies_tabel_4 = dv_policies_tabel_4-rafm_policies_tabel_4
diff_sa_tabel_4 = dv_sa_tabel_4-rafm_sa_tabel_4

summary_tabel_4 = pd.DataFrame([{
    "DV": dv_policies_tabel_4,
    "DV SA": dv_sa_tabel_4,
    "RAFM Output": rafm_policies_tabel_4, 
    "RAFM SA": rafm_sa_tabel_4,
    'Diff # of Policies':diff_policies_tabel_4,
    'Diff SA': diff_sa_tabel_4
}])

summary_tabel_4