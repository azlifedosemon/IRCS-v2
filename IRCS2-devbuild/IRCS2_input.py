import pandas as pd
import openpyxl

CODE_LIBRARY_path = r'D:\1. IRCS Automation\Control 2 DEV\IRCS-v2\IRCS2-devbuild\source\Input Sheet.xlsx'

input_df = (pd.read_excel(CODE_LIBRARY_path, engine='openpyxl', sheet_name=['PATH INPUT']))['PATH INPUT']
path_map = dict(zip(input_df['Category'], input_df['Path']))

DV_AZTRAD_path       = path_map.get('DV_AZTRAD')
DV_AZUL_path         = path_map.get('DV_AZUL')
IT_AZTRAD_path       = path_map.get('IT_AZTRAD')
IT_AZUL_path         = path_map.get('IT_AZUL')
xlsx_filename        = path_map.get('Output filename')
xlsx_output          = "\\".join([x for x in DV_AZTRAD_path.split('\\')]
                                 [:len(DV_AZTRAD_path.split('\\')) - 1 ]) + "\\" + xlsx_filename + ".xlsx"

user_input = [DV_AZTRAD_path, DV_AZUL_path, IT_AZTRAD_path, IT_AZUL_path, CODE_LIBRARY_path, xlsx_output]