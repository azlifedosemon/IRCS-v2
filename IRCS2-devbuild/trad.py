import pandas as pd
import numpy as np
import csv
from datetime import datetime
from functools import reduce
import IRCS2_input as input_sheet


code = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["TRAD"],engine="openpyxl")
code = code["TRAD"]

trad_dv = pd.read_csv(input_sheet.DV_AZTRAD_path,sep = ",")

trad_dv1 = trad_dv.drop(columns=["goc"])
trad_dv_final = trad_dv1.groupby(["product_group"],as_index=False).sum(numeric_only=True)
trad_dv_final[["product", "currency"]] = trad_dv_final["product_group"].str.extract(r"(\w+)_([\w\d]+)")
trad_dv_final.drop(columns="product_group")

original_trad = trad_dv_final.copy()

convert = dict(zip(code["Prophet Code"], code["Flag Code"]))
trad_dv_final["product"] = trad_dv_final["product"].map(convert).fillna(trad_dv_final["product"])
trad_dv_final["product_group"]= trad_dv_final["product"].str.cat(trad_dv_final["currency"], sep="_")
trad2 = trad_dv_final.copy()

trad_dv_final["pol_num"] = (
    trad_dv_final["pol_num"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["pol_num"] = pd.to_numeric(
    trad_dv_final["pol_num"], errors="coerce"
)

trad_dv_final["pre_ann"] = (
    trad_dv_final["pre_ann"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["pre_ann"] = pd.to_numeric(
    trad_dv_final["pre_ann"], errors="coerce"
)


trad_dv_final["sum_assd"] = (
    trad_dv_final["sum_assd"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["sum_assd"] = pd.to_numeric(
    trad_dv_final["sum_assd"], errors="coerce"
)

trad_dv_final["loan_sa"] = (
    trad_dv_final["loan_sa"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

trad_dv_final["loan_sa"] = pd.to_numeric(
    trad_dv_final["loan_sa"], errors="coerce"
)

trad_dv_final = trad_dv_final.groupby(["product_group"],as_index=False).sum(numeric_only=True)
trad_dv_final = trad_dv_final[~(trad_dv_final["product_group"].str.startswith("A_"))]
trad_dv_final
pol_e_trad_dv_final = sum(trad_dv_final["pol_num"])
sa_if_m_trad_dv_final = sum(trad_dv_final["sum_assd"])
anp_if_m_trad_dv_final = sum(trad_dv_final["pre_ann"])

summary_trad_dv_final = pd.DataFrame([{
    "pol_e": pol_e_trad_dv_final,
    "sa_if_m": sa_if_m_trad_dv_final,
    "anp_if_m": anp_if_m_trad_dv_final,
}])

# print(summary_trad_dv_final)


full_stat = pd.read_csv(input_sheet.IT_AZTRAD_path, sep = ";")

full_stat["product_group"] = full_stat["PRODUCT_CODE"].str.replace("BASE_","",regex=False)+"_"+full_stat["CURRENCY1"]
full_stat = full_stat.drop(columns=["PRODUCT_CODE","CURRENCY1"])
trad_stat = full_stat.copy()

full_stat["POLICY_REF_Count"] = (
    full_stat["POLICY_REF_Count"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["POLICY_REF_Count"] = pd.to_numeric(
    full_stat["POLICY_REF_Count"], errors="coerce"
)

full_stat["pre_ann_Sum"] = (
    full_stat["pre_ann_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["pre_ann_Sum"] = pd.to_numeric(
    full_stat["pre_ann_Sum"], errors="coerce"
)


full_stat["sum_assd_Sum"] = (
    full_stat["sum_assd_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

full_stat["sum_assd_Sum"] = pd.to_numeric(
    full_stat["sum_assd_Sum"], errors="coerce"
)

full_stat = full_stat.groupby(["product_group"],as_index=False).sum(numeric_only=True)
full_stat = full_stat[~(full_stat["product_group"].str.startswith("A_") | full_stat["product_group"].str.startswith("NA_"))]

full_stat

summary = pd.read_csv(input_sheet.SUMMARY_path, sep = ",")
summary["product_group"] = summary["prod_code_First"]+"_"+summary["currency_First"]
summary = summary.drop(columns=["prod_code_First","currency_First"])
summary = summary.rename(columns={"pol_num_Count":"POLICY_REF_Count" })

summary["POLICY_REF_Count"] = (
    summary["POLICY_REF_Count"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

summary["POLICY_REF_Count"] = pd.to_numeric(
    summary["POLICY_REF_Count"], errors="coerce"
)

summary["pre_ann_Sum"] = (
    summary["pre_ann_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

summary["pre_ann_Sum"] = pd.to_numeric(
    summary["pre_ann_Sum"], errors="coerce"
)


summary["sum_assd_Sum"] = (
    summary["sum_assd_Sum"]
    .astype(str)                                
    .str.replace(",", ".", regex=False)         
)

summary["sum_assd_Sum"] = pd.to_numeric(
    summary["sum_assd_Sum"], errors="coerce"
)

summary = summary.groupby(["product_group"],as_index=False).sum(numeric_only=True)

summary

mapping_dict = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["SPEC TRAD"],engine="openpyxl")
mapping_dict = mapping_dict["SPEC TRAD"]

mapping_dict


full_stat_total = pd.concat([full_stat,summary])
full_stat_total[["product", "currency"]] = full_stat_total["product_group"].str.extract(r"(\w+)_([\w\d]+)")
full_stat_total = full_stat_total.copy()
convert = dict(zip(mapping_dict["Old"], mapping_dict["New"]))
full_stat_total["product"] = full_stat_total["product"].map(convert).fillna(full_stat_total["product"])
full_stat_total["product_group"] = full_stat_total["product"].str.cat(full_stat_total["currency"], sep="_")
full_stat_total = full_stat_total.drop(columns=["product","currency"])
full_stat_total = full_stat_total.groupby(["product_group"],as_index=False).sum(numeric_only=True)


# print(full_stat_total)

pol_e_full_stat_total = sum(full_stat_total["POLICY_REF_Count"])
sa_if_m_full_stat_total = sum(full_stat_total["sum_assd_Sum"])
anp_if_m_full_stat_total = sum(full_stat_total["pre_ann_Sum"])

summary_full_stat_total = pd.DataFrame([{
    "pol_e": pol_e_full_stat_total,
    "sa_if_m": sa_if_m_full_stat_total,
    "anp_if_m": anp_if_m_full_stat_total
}])

campaign = pd.read_csv(input_sheet.LGC_LGM_CAMPAIGN_path,sep=";")
campaign = campaign.drop(columns=["campaign_Period"])
campaign

tradcon_input = pd.read_csv(input_sheet.TRADCONV_path, sep=';', encoding='utf-8', quoting=csv.QUOTE_NONE, on_bad_lines='skip')
tradcon_input = tradcon_input[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradcon_input = tradcon_input[tradcon_input['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradcon_input = tradcon_input.groupby(["POLICY_REF"]).first().reset_index()
tradcon_input

def filter_by_quarter(input, reporting_quarter,financial_year):
   
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

    input['POLICY_START_DATE'] = pd.to_datetime(input['POLICY_START_DATE'], format='%d-%b-%y')

    filtered = input[input['POLICY_START_DATE'] < cutoff]

    return filtered

tradcon_cleaned = filter_by_quarter(tradcon_input, input_sheet.reporting_quarter, input_sheet.financial_year)

tradsha_input = pd.read_csv(input_sheet.TRADSHA_path, sep=';', encoding='utf-8', quoting=csv.QUOTE_NONE, on_bad_lines='skip')
tradsha_input = tradsha_input[['POLICY_REF','PRODUCT_CODE','COVER_CODE','SUM_INSURED','CURRENCY1','POLICY_START_DATE']]
tradsha_input = tradsha_input[tradsha_input['PRODUCT_CODE'].str.contains('lg[cm]', case=False, na=False)]
tradsha_input = tradsha_input.groupby(["POLICY_REF"]).first().reset_index()
tradsha_input

tradsha_cleaned = filter_by_quarter(tradsha_input,input_sheet.reporting_quarter, input_sheet.financial_year)

# spacer

tradcon = tradcon_cleaned
tradcon = tradcon.drop(columns=["POLICY_START_DATE"])

tradsha = tradsha_cleaned
tradsha = tradsha.drop(columns=["POLICY_START_DATE"])

merged_trad = pd.concat([tradcon,tradsha])
merged_trad
campaign_total = campaign.merge(merged_trad, 
                   left_on="Policy No", 
                   right_on="POLICY_REF", 
                   how="left")
campaign_total = campaign_total.fillna(0)
campaign_total = campaign_total.drop("POLICY_REF", axis=1)

lookup = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["Campaign Lookup"],engine="openpyxl")
lookup = lookup["Campaign Lookup"]
campaign_total["SUM_INSURED"] = pd.to_numeric(campaign_total["SUM_INSURED"], errors="coerce")
lookup["Max Bonus"] = pd.to_numeric(lookup["Max Bonus"], errors="coerce")

campaign_total["key"] = campaign_total["campaign_type"].astype(str) + "_" + campaign_total["CURRENCY1"].astype(str)
bonus = campaign_total.merge(lookup[["key", "Max Bonus"]], on="key", how="left")

bonus["calculated_bonus"] = bonus["SUM_INSURED"] * 0.1
bonus["Bonus SA"] = np.where(
    bonus["Max Bonus"].notna(),
    np.minimum(bonus["calculated_bonus"], bonus["Max Bonus"]),
    0
)

bonus["SA After Bonus"] = bonus["SUM_INSURED"]+bonus["Bonus SA"]

bonus = bonus.drop(["key", "calculated_bonus","Max Bonus"], axis=1)

summary = bonus.drop(columns=["Policy No","campaign_type","product","PRODUCT_CODE"])
summary["Grouping Raw Data"] = summary["COVER_CODE"].str.replace("BASE_","",regex=False)+"_"+summary["CURRENCY1"]
summary = summary.groupby(["Grouping Raw Data"],as_index=False).sum(numeric_only=True)

print(summary)

summary[["product", "currency"]] = summary["Grouping Raw Data"].str.extract(r"(\w+)_([\w\d]+)")
convert = dict(zip(code["Flag Code"], code["Prophet Code"]))
summary["Grouping DV"] = summary["product"].map(convert).fillna(summary["product"])
summary = summary.drop(columns=["product","currency"])
cols = ["Grouping Raw Data", "Grouping DV"] + [col for col in summary.columns if col not in ["Grouping Raw Data", "Grouping DV"]]
summary = summary[cols]


bsi = pd.read_excel(input_sheet.BSI_ATTRIBUSI_path, sheet_name = ["Export Worksheet"], engine="openpyxl")
bsi = bsi["Export Worksheet"]
bsi = bsi.drop(columns=["POLICY_NO","CP_PH_ID","CP_PH","PRODUCT_CODE","CP_INSURED_ID","LOANNO","CP_INSURED","POLICY_STATUS","UP_ATTR"])
bsi = bsi.rename(columns = {"COVER_CODE":"product",
                            "PREM_ATTR":"anp"})
code_bsi = pd.read_excel(input_sheet.CODE_LIBRARY_path,sheet_name = ["Code BSI"],engine="openpyxl")
code_bsi = code_bsi["Code BSI"]
convert = dict(zip(code_bsi["Cover_code"], code_bsi["Grouping raw data"]))
bsi["product_group"] = bsi["product"].map(convert).fillna(bsi["product"])
bsi = bsi.groupby(["product_group"],as_index=False).sum(numeric_only=True)
bsi["product_group"] = bsi["product_group"]+"_IDR"

summary = summary.rename(columns = {"Grouping Raw Data" : "product_group","Bonus SA":"sum_assd"})
summary = summary.drop(columns = {"Grouping DV","SUM_INSURED","SA After Bonus"})

dfs = [trad_dv_final, full_stat_total, summary]

merged = reduce(lambda left, right: pd.merge(left, right, 
                                              on='product_group', 
                                              how='outer'), dfs)


merged.fillna(0, inplace=True)

result = pd.DataFrame()
result["product_group"] = merged["product_group"]
result["policy_count_diff"] = merged["pol_num"] - merged["POLICY_REF_Count"]
result["sum_a_if_m_diff"] = merged["sum_assd_x"] - merged["sum_assd_Sum"] - merged["sum_assd_y"]
result["anp_if_m_diff"] = merged["pre_ann"] - merged["pre_ann_Sum"]
result
merged_2 = pd.merge(result, bsi, on="product_group", how="outer", 
                  suffixes=("_result", "_bsi"))

merged_2.fillna(0, inplace=True)

total = pd.DataFrame()
total["product_group"] = merged_2["product_group"]
total["policy_count_diff"] = merged_2["policy_count_diff"]
total["sum_a_if_m_diff"] = merged_2["sum_a_if_m_diff"]
total["anp_if_m_diff"] = merged_2["anp_if_m_diff"] + merged_2["anp"]

diff_pol_e_input = pol_e_trad_dv_final-pol_e_full_stat_total
diff_sa_if_m_input = sa_if_m_trad_dv_final-sa_if_m_full_stat_total
diff_anp_if_m_input = anp_if_m_trad_dv_final-anp_if_m_full_stat_total

summary_diff_total_input = pd.DataFrame([{
    "pol_e_input": diff_pol_e_input,
    "sa_if_m_input": diff_sa_if_m_input,
    "anp_if_m_input": diff_anp_if_m_input,
}])

# print(summary_diff_total_input)
policy_count_diff_output = sum(total["policy_count_diff"])
pre_ann_diff_aztrad_output= sum(total["anp_if_m_diff"])-sum(bsi["anp"])
sum_assur_diff_aztrad_output= sum(total["sum_a_if_m_diff"])+sum(summary["sum_assd"])

sum_diff_aztrad_output = pd.DataFrame([{
    "policy_count_aztrad_output": policy_count_diff_output,
    "sa_if_m_aztrad_output": sum_assur_diff_aztrad_output,
    "anp_if_m_aztrad_output": pre_ann_diff_aztrad_output, 
}])

# print(sum_diff_aztrad_output)

policy_count_diff_aztrad = sum(total["policy_count_diff"])
pre_ann_diff_aztrad= sum(total["anp_if_m_diff"])
sum_assur_diff_aztrad= sum(total["sum_a_if_m_diff"])

sum_diff_aztrad = pd.DataFrame([{
    "policy_count_aztrad": policy_count_diff_aztrad,
    "sa_if_m_aztrad": sum_assur_diff_aztrad,
    "anp_if_m_aztrad": pre_ann_diff_aztrad, 
}])

(sum_diff_aztrad)

merged_3 = pd.merge(total, full_stat_total, on="product_group", how="outer", 
                  suffixes=("_total", "_full_stat_total"))

merged_3.fillna(0, inplace=True)

result_percent = pd.DataFrame()
result_percent["product_group"] = merged_3["product_group"]
result_percent["policy_count_percent"] = merged_3["policy_count_diff"]/merged_3["POLICY_REF_Count"]*100
result_percent["pre_ann_percent"] = merged_3["anp_if_m_diff"]/merged_3["pre_ann_Sum"]*100
result_percent["sum_assur_percent"] = merged_3["sum_a_if_m_diff"] /merged_3["sum_assd_Sum"]*100

# print(result_percent) full diff percentage

policy_count = ((sum_diff_aztrad["policy_count_aztrad"]/summary_full_stat_total["pol_e"])*100) 
sa_if_m= (sum_diff_aztrad["sa_if_m_aztrad"]/summary_full_stat_total["sa_if_m"])*100
anp_if_m =(sum_diff_aztrad["anp_if_m_aztrad"]/summary_full_stat_total["anp_if_m"])*100

Different_Percentage = pd.DataFrame([{
    "policy_count": policy_count,
    "sa_if_m": sa_if_m,
    "anp_if_m": anp_if_m
}])

# print(Different_Percentage) summary diff percentage