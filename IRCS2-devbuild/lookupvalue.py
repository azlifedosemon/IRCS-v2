import pandas as pd
import IRCS2_input as input_script
import xlsxwriter
import UL

ul_dv = pd.read_csv(input_script.DV_AZUL_path)
ul_dv = ul_dv.drop(columns=["goc"])
ul_dv_final = ul_dv.groupby(["product_group"],as_index=False).sum(numeric_only=True)
# print(ul_dv_final)

code_ul = pd.read_excel(input_script.CODE_LIBRARY_path,sheet_name = ["UL"],engine="openpyxl")
code_ul = code_ul["UL"]
# print(code_ul)

ul_dv_final[["product", "currency"]] = ul_dv_final["product_group"].str.extract(r"(\w+)_([\w\d]+)")
ul_dv_final = ul_dv_final.drop(columns="product_group")
 
a1 = (ul_dv_final[["product",'currency']])
convert = dict(zip(code_ul["Prophet Code"], code_ul["Flag Code"]))
ul_dv_final["product"] = ul_dv_final["product"].map(convert).fillna(ul_dv_final["product"])
a2 = (ul_dv_final[['product','currency']])
ul_dv_final["product_group"] = ul_dv_final["product"].str.cat(ul_dv_final["currency"], sep="_")

a1['product code'] = (
    a1['product']
      .str.rstrip('_')        
      .str.cat(a1['currency'], sep='_')
)

a2 ['product code'] =(
    a2['product'] + '_' + a2['currency']
) 

lookup = pd.DataFrame({
    'Product code':        a1['product'],
    'Grouping DV':         a1['product code'],
    'product_group':   a2['product code']
})

merged = (
    lookup
      .groupby('product_group', sort=False)
      .agg({
         'Product code': '/'.join,
         'Grouping DV':  '/'.join
      })
      .reset_index()
)

partial_lookup = UL.table2
full_lookup_table = pd.merge(merged, partial_lookup, on="product_group", how='right')
# 1) Pick your “fixed” first three
first_three = ['Product code', 'Grouping DV', 'product_group']

# 2) Grab the rest in their existing order
rest = [c for c in full_lookup_table.columns if c not in first_three]

# 3) Reindex into the new order
full_lookup_table = full_lookup_table[first_three + rest]

