import os
import sys
import pandas as pd

if len(sys.argv) != 3:
    print("Usage: python dv_worker.py <path_to_excel> <out_pickle>")
    sys.exit(1)

path, out_pickle = sys.argv[1], sys.argv[2]

# print(f"Reading Excel: {path}")
# print(f"Will write Pickle to: {out_pickle}")
# print(f"Worker CWD: {os.getcwd()}")  # Show current working directory

# Read, drop redundancy, save as Pickle
df = pd.read_excel(path, engine='openpyxl')
df.columns = [str(col).strip() for col in df.columns]
cols_to_drop = (
    ['product_group', 'pre_ann', 'loan_sa', 'sum_assur']
    + [c for c in df.columns if str(c).startswith('Unnamed')]
)
df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

try:
    df.to_pickle(out_pickle)
    # print(f"Pickle successfully written: {out_pickle}")
except Exception as e:
    print("ERROR while saving pickle:", e)
    sys.exit(2)
