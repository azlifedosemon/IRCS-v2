import pandas as pd
import csv
import numpy as np
from datetime import datetime

tradcon_input = pd.read_csv('D:\IRCS\Control 2\LGC & LGM Campaign\RESERVE_TRADCONV_RWNB_IFRS_20250402.txt', sep=';', encoding='utf-8', quoting=csv.QUOTE_NONE, on_bad_lines='skip')
tradcon_input = tradcon_input[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradcon_input = tradcon_input[tradcon_input['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradcon_input = tradcon_input.groupby(["POLICY_REF"]).first().reset_index()
tradcon_input

def filter_by_quarter(tradcon_input, reporting_quarter,financial_year):
   
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

    tradcon_input['POLICY_START_DATE'] = pd.to_datetime(tradcon_input['POLICY_START_DATE'], format='%d-%b-%y')

    filtered = tradcon_input[tradcon_input['POLICY_START_DATE'] < cutoff]

    return filtered

tradcon_cleaned = filter_by_quarter(tradcon_input, input_sheet.reporting_quarter, input_sheet.financial_year)

tradsha_input = pd.read_csv("D:\IRCS\Control 2\LGC & LGM Campaign\RESERVE_TRADSHA_RWNB_IFRS_20250402.txt", sep=';', encoding='utf-8', quoting=csv.QUOTE_NONE, on_bad_lines='skip')
tradsha_input = tradsha_input[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradsha_input = tradsha_input[tradsha_input['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradsha_input = tradsha_input.groupby(["POLICY_REF"]).first().reset_index()
tradsha_input
def filter_by_quarter(tradsha_input,reporting_quarter,financial_year):
   
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

    tradsha_input['POLICY_START_DATE'] = pd.to_datetime(tradsha_input['POLICY_START_DATE'], format='%d-%b-%y')

    filtered = tradsha_input[tradsha_input['POLICY_START_DATE'] < cutoff]

    return filtered

tradsha_cleaned = filter_by_quarter(tradsha_input,input_sheet.reporting_quarter, input_sheet.financial_year)
