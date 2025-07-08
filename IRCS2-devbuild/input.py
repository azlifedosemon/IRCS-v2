import pandas as pd
import csv
import numpy as np
from datetime import datetime

tradcon = pd.read_csv('D:\IRCS\Control 2\LGC & LGM Campaign\RESERVE_TRADCONV_RWNB_IFRS_20250402.txt', sep=';', encoding='utf-8', quoting=csv.QUOTE_NONE, on_bad_lines='skip')
tradcon = tradcon[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradcon = tradcon[tradcon['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradcon = tradcon.groupby(["POLICY_REF"]).first().reset_index()
tradcon
def filter_by_quarter(tradcon, reporting_quarter,financial_year):
   
    quarter = reporting_quarter
    year = financial_year
    
    if quarter == 1:
        cutoff = datetime(year, 4, 1)
    elif quarter == 2:
        cutoff = datetime(year, 7, 1) 
    elif quarter == 3:
        cutoff = datetime(year, 10, 1)  
    elif quarter == 4:
        cutoff = datetime(year + 1, 1, 1) 
    else:
        raise ValueError("Kuartal harus antara 1 sampai 4")

    tradcon['POLICY_START_DATE'] = pd.to_datetime(tradcon['POLICY_START_DATE'], format='%d-%b-%y')

    filtered = tradcon[tradcon['POLICY_START_DATE'] < cutoff]

    return filtered

reporting_quarter = 1
financial_year = 2025
hasil = filter_by_quarter(tradcon, reporting_quarter,financial_year)
print(hasil)
tradsha = pd.read_csv("D:\IRCS\Control 2\LGC & LGM Campaign\RESERVE_TRADSHA_RWNB_IFRS_20250402.txt", sep=';', encoding='utf-8', quoting=csv.QUOTE_NONE, on_bad_lines='skip')
tradsha = tradsha[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradsha = tradsha[tradsha['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradsha = tradsha.groupby(["POLICY_REF"]).first().reset_index()
tradsha
def filter_by_quarter(tradsha,reporting_quarter,financial_year):
   
    quarter = reporting_quarter
    year = financial_year

    
    if quarter == 1:
        cutoff = datetime(year, 4, 1)
    elif quarter == 2:
        cutoff = datetime(year, 7, 1) 
    elif quarter == 3:
        cutoff = datetime(year, 10, 1)  
    elif quarter == 4:
        cutoff = datetime(year + 1, 1, 1) 
    else:
        raise ValueError("Kuartal harus antara 1 sampai 4")

    tradsha['POLICY_START_DATE'] = pd.to_datetime(tradsha['POLICY_START_DATE'], format='%d-%b-%y')

    filtered = tradsha[tradsha['POLICY_START_DATE'] < cutoff]

    return filtered
reporting_quarter = 1
financial_year = 2025
hasil = filter_by_quarter(tradsha,reporting_quarter,financial_year)
print(hasil)