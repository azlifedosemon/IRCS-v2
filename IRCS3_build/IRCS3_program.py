import pandas as pd
import numpy as np
import xlsxwriter
import IRCS3_input as input_sheet
import trad_processing as trad
import ul_processing as ul
import time

def elapsed_time(start,end,script):
    if round((end - start),0) > 60:
        print(f"\n• {script} RUNTIME: {round((end - start) / 60, 2)} minutes", end='')
    elif (end - start) < 1:
        print(f"\n• {script} RUNTIME: {round((end - start) * 1000, 2)} ms", end= '')
    else:
        print(f"\n• {script} RUNTIME: {round((end - start), 2)} second", end= '')
    
start_time = time.time()


if input_sheet.bool_trad:
    ################################ EXCEL INPUT ################################
    wb = xlsxwriter.Workbook(input_sheet.excel_output_trad, {'nan_inf_to_errors': True})
    number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'

    ################################ CONTROL SUMMARY ################################
    header_sum_tablerow = ['DV', 'RAFM', 'Differences']
    header_sum_tablerow2 = ['Total', 'Trad-Life inc. BTPN', 'Trad-Health non-YRT', 'Trad-Health YRT', 'Trad-C']
    tablerow2_len = len(header_sum_tablerow2)
    
    ws = wb.add_worksheet('Control and Summary')

    ws.freeze_panes(0, 1)

    ws.set_column(0, 0, 20)
    ws.set_column(1, 12, 25)
    ws.set_column(13, 13, 30)

    ws.write(0, 0, 'Valuation Year', wb.add_format({'bold': True}))
    ws.write(1, 0, 'Valuation Month', wb.add_format({'bold': True}))
    ws.write(2, 0, 'FX Rate ValDate', wb.add_format({'bold': True}))
    ws.write(4, 0, '# of Policies Check', wb.add_format({'bold': True, 'underline': True, 'bg_color': 'green'}))
    ws.write(5, 0, '# Run', wb.add_format({'bold': True, 'underline': True}))

    ws.write(0, 1, input_sheet.valuation_year, wb.add_format({'bold': True, 'bg_color': 'yellow'}))
    ws.write(1, 1, input_sheet.valuation_month, wb.add_format({'bold': True, 'bg_color': 'yellow', 'align': 'right'}))
    ws.write(2, 1, input_sheet.valuation_rate, wb.add_format({'bold': True, 'bg_color': 'yellow'}))


    for i, key in enumerate(trad.tradfilter):
        ws.write(6 + i, 0, key, wb.add_format({'bold': True, 'bg_color': 'yellow'}))

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (tablerow2_len * c), 4, tablerow2_len + (tablerow2_len * c), item, wb.add_format({'bold': True, 'align': 'center'}))

    ws.merge_range(4, 16, 5, 16, 'Notes', wb.add_format({'bold': True, 'align': 'center'}))

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (tablerow2_len * i), item, wb.add_format({'bold': True, 'align':'center'}))

    for i, run_name in enumerate(trad.tradfilter):
        ctrlsum = pd.DataFrame([trad.ctrlsum_dict[run_name]])

        for c, item_ in enumerate(ctrlsum.iloc[0]):
            ws.write(6 + i, c + 1, item_, wb.add_format({'num_format': number_format}))


    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    ################################ DIFF BREAKDOWN ################################
    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV SA', 'RAFM # of Policies', 'RAFM SA', 'Diff # of Policies', 'Diff SA']
    header_diff_tablecol = ['Total All from DV', 'Grand Total Summary', 'Check']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color':'#92D050'})


    for run_name in trad.tradfilter:
        ws          = wb.add_worksheet(f'{run_name}')
        df1         = trad.table1_df[run_name]
        df2         = trad.table2_df[run_name]
        df3         = trad.table3_df[run_name]
        df4         = trad.table4_df[run_name]
        df5         = trad.table5_df[run_name]
        summary     = trad.summary_dict[run_name]
        summary2    = pd.DataFrame([trad.table2_sum[run_name]])
        summary3    = pd.DataFrame([trad.table3_sum[run_name]])
        summary4    = pd.DataFrame([trad.table4_sum[run_name]])
        summary5    = pd.DataFrame([trad.table5_sum[run_name]])
        
        ################################ FORMATTING ################################    
        
        ws.set_column(1, 1, 40)    
        ws.set_column(2, 7, 20)
        ws.set_column(9, 15, 20)
        ws.set_column(9, 9, 40)
        ws.set_column(17, 23, 20)
        ws.set_column(17, 17, 40)
        ws.set_column(25, 31, 20)
        ws.set_column(25, 25, 40)
        ws.set_column(33, 39, 20)
        ws.set_column(33, 33, 40)
        
        for c, item_ in enumerate(header_diff_tablerow):
            ws.write(2, 1 + c, item_, wb.add_format({'bold': True, 'underline': True}))
            ws.write(2, 9 + c, item_, wb.add_format({'bold': True, 'underline': True}))
            ws.write(2, 17 + c, item_, wb.add_format({'bold': True, 'underline': True}))
            ws.write(2, 25 + c, item_, wb.add_format({'bold': True, 'underline': True}))
            ws.write(2, 33 + c, item_, wb.add_format({'bold': True, 'underline': True}))
                                
        for r, item_ in enumerate(header_diff_tablecol):
            ws.write(3 + r, 1, item_, tablecol_fmt)
            ws.write(3 + r, 9, item_, tablecol_fmt)
            ws.write(3 + r, 17, item_, tablecol_fmt)
            ws.write(3 + r, 25, item_, tablecol_fmt)
            ws.write(3 + r, 33, item_, tablecol_fmt)
        
        ws.write(3, 9, 'Total BTPN', tablecol_fmt)
        ws.write(3, 17, 'Total Health non-YRT', tablecol_fmt)
        ws.write(3, 25, 'Total Health YRT', tablecol_fmt)
        ws.write(3, 33, 'Total C', tablecol_fmt)

            
        ################################ FORMATTING ################################
        
        for row in range(len(summary)):
            for c, item_ in enumerate(summary.iloc[row]):
                ws.write(3 + row, 2 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
        
        for row in range(len(df1)):
            for c, item_ in enumerate(df1.iloc[row]):
                ws.write(6 + row, 1 + c, item_, wb.add_format({'num_format': number_format}))

        for row in range(len(summary2)):
            for c, item_ in enumerate(summary2.iloc[row]):
                ws.write(3 + row, 10 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(4, 10 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(5, 10 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))

        for row in range(len(df2)):
            for c, item_ in enumerate(df2.iloc[row]):
                ws.write(6 + row, 9 + c, item_, wb.add_format({'num_format': number_format}))

        for row in range(len(summary3)):
            for c, item_ in enumerate(summary3.iloc[row]):
                ws.write(3 + row, 18 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(4, 18 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(5, 18 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                
        for row in range(len(df3)):
            for c, item_ in enumerate(df3.iloc[row]):
                ws.write(6 + row, 17 + c, item_, wb.add_format({'num_format': number_format}))

        for row in range(len(summary4)):
            for c, item_ in enumerate(summary4.iloc[row]):
                ws.write(3 + row, 26 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(4, 26 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(5, 26 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                
        for row in range(len(df4)):
            for c, item_ in enumerate(df4.iloc[row]):
                ws.write(6 + row, 25 + c, item_, wb.add_format({'num_format': number_format}))
        
        for row in range(len(summary5)):
            for c, item_ in enumerate(summary5.iloc[row]):
                ws.write(3 + row, 34 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(4, 34 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(5, 34 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                
        for row in range(len(df5)):
            for c, item_ in enumerate(df5.iloc[row]):
                ws.write(6 + row, 33 + c, item_, wb.add_format({'num_format': number_format}))

    wb.close()


if input_sheet.bool_ul:
    ################################ EXCEL INPUT ################################
    wb = xlsxwriter.Workbook(input_sheet.excel_output_ul, {'nan_inf_to_errors': True})
    number_format = '_(* #,##0_);_(* (#,##0)_);_(* "-"_);_(@_)'
    
    ################################ CONTROL SUMMARY ################################
    header_sum_tablerow = ['DV', 'RAFM', 'Differences']
    header_sum_tablerow2 = ['Total', 'UL & SH & PI', 'Tasbih', 'GS']

    ws = wb.add_worksheet('Control and Summary')

    ws.freeze_panes(0, 1)

    ws.set_column(0, 0, 20)
    ws.set_column(1, 12, 25)
    ws.set_column(13, 13, 30)

    ws.write(0, 0, 'Valuation Year', wb.add_format({'bold': True}))
    ws.write(1, 0, 'Valuation Month', wb.add_format({'bold': True}))
    ws.write(2, 0, 'FX Rate ValDate', wb.add_format({'bold': True}))
    ws.write(4, 0, '# of Policies Check', wb.add_format({'bold': True, 'underline': True, 'bg_color': 'green'}))
    ws.write(5, 0, '# Run', wb.add_format({'bold': True, 'underline': True}))

    ws.write(0, 1, input_sheet.valuation_year, wb.add_format({'bold': True, 'bg_color': 'yellow'}))
    ws.write(1, 1, input_sheet.valuation_month, wb.add_format({'bold': True, 'bg_color': 'yellow', 'align': 'right'}))
    ws.write(2, 1, input_sheet.valuation_rate, wb.add_format({'bold': True, 'bg_color': 'yellow'}))


    for i, key in enumerate(input_sheet.ulfilter):
        ws.write(6 + i, 0, key, wb.add_format({'bold': True, 'bg_color': 'yellow'}))

    for c, item in enumerate(header_sum_tablerow):
        ws.merge_range(4, 1 + (4 * c), 4, 4 + (4 * c), item, wb.add_format({'bold': True, 'align': 'center'}))

    ws.merge_range(4, 13, 5, 13, 'Notes', wb.add_format({'bold': True, 'align': 'center'}))

    for i in range(len(header_sum_tablerow)):
        for c, item in enumerate(header_sum_tablerow2):
            ws.write(5, c + 1 + (4 * i), item, wb.add_format({'bold': True, 'align':'center'}))

    for i, run_name in enumerate(input_sheet.ulfilter):
        ctrlsum = pd.DataFrame([ul.ctrlsum_dict[run_name]])

        for c, item_ in enumerate(ctrlsum.iloc[0]):
            ws.write(6 + i, c + 1, item_, wb.add_format({'num_format': number_format}))
    
    wb.add_worksheet('Diff Breakdown')
    wb.add_worksheet('>>')

    ################################ DIFF BREAKDOWN ################################
    header_diff_tablerow = ['GOC', 'DV # of Policies', 'DV Fund Value', 'RAFM # of Policies', 'RAFM Fund Value', 'Diff # of Policies', 'Diff Fund Value']
    header_diff_tablecol = ['Total All from DV', 'Grand Total Summary', 'Check']
    tablecol_fmt = wb.add_format({'bold': True, 'underline': True, 'bg_color':'#92D050'})


    for run_name in input_sheet.ulfilter:
        ws          = wb.add_worksheet(f'{run_name}')
        df1         = ul.table1_df[run_name]
        df2         = ul.table2_df[run_name]
        df3         = ul.table3_df[run_name]
        
        summary     = ul.summary_dict[run_name]
        summary2    = pd.DataFrame([ul.table2_sum[run_name]])
        summary3    = pd.DataFrame([ul.table3_sum[run_name]])
        
        ################################ FORMATTING ################################    
        
        ws.set_column(1, 1, 40)    
        ws.set_column(2, 7, 20)
        ws.set_column(9, 15, 20)
        ws.set_column(9, 9, 40)
        ws.set_column(17, 23, 20)
        ws.set_column(17, 17, 40)
 
        
        for c, item_ in enumerate(header_diff_tablerow):
            ws.write(2, 1 + c, item_, wb.add_format({'bold': True, 'underline': True}))
            ws.write(2, 9 + c, item_, wb.add_format({'bold': True, 'underline': True}))
            ws.write(2, 17 + c, item_, wb.add_format({'bold': True, 'underline': True}))
                                
        for r, item_ in enumerate(header_diff_tablecol):
            ws.write(3 + r, 1, item_, tablecol_fmt)
            ws.write(3 + r, 9, item_, tablecol_fmt)
            ws.write(3 + r, 17, item_, tablecol_fmt)
        
        ws.write(3, 9, 'Total Tasbih', tablecol_fmt)
        ws.write(3, 17, 'Total Group Savings', tablecol_fmt)

            
        ################################ FORMATTING ################################
        
        for row in range(len(summary)):
            for c, item_ in enumerate(summary.iloc[row]):
                ws.write(3 + row, 2 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
        
        for row in range(len(df1)):
            for c, item_ in enumerate(df1.iloc[row]):
                ws.write(6 + row, 1 + c, item_, wb.add_format({'num_format': number_format}))

        for row in range(len(summary2)):
            for c, item_ in enumerate(summary2.iloc[row]):
                ws.write(3 + row, 10 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(4, 10 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(5, 10 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))

        for row in range(len(df2)):
            for c, item_ in enumerate(df2.iloc[row]):
                ws.write(6 + row, 9 + c, item_, wb.add_format({'num_format': number_format}))

        for row in range(len(summary3)):
            for c, item_ in enumerate(summary3.iloc[row]):
                ws.write(3 + row, 18 + c, item_, wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(4, 18 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                ws.write(5, 18 + c, "", wb.add_format({'num_format': number_format, 'bg_color': '#92D050', 'bold': True}))
                
        for row in range(len(df3)):
            for c, item_ in enumerate(df3.iloc[row]):
                ws.write(6 + row, 17 + c, item_, wb.add_format({'num_format': number_format}))
        
    wb.close()
    
    
    
end_time = time.time()
print("DIAGNOSTICS:", end='')
elapsed_time(input_sheet.start_time, input_sheet.end_time, 'INPUT')
elapsed_time(trad.start_time, ul.end_time, 'DATA PROCESSING')
elapsed_time(start_time, end_time, "OUTPUT")
elapsed_time(input_sheet.start_time, end_time, "CUMULATIVE")