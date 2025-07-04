import xlsxwriter
from collections import defaultdict


output_path = "D:/1. IRCS Automation/Script Test/test script2/frozen_row.xlsx"
wb = xlsxwriter.Workbook(output_path)
ws = wb.add_worksheet('Data')

ws.freeze_panes(10, 4)

headers_summary = ['Items', 'Total Input from csv', 'Total output in summary', 'Diff', 'AZUL']

headers_sum_dict = defaultdict(int)
for h in headers_summary:
    headers_sum_dict[h] = len(h)
max_len = max(headers_sum_dict.items())[1]
ws.set_column(3, 21, max_len + 2)

for c, h in enumerate(headers_summary):
    if c < len(headers_sum_dict) - 1:
        ws.write(c + 1, 3, h, wb.add_format({'bold': True}))
    else:
        ws.write(c + 1, 3, h, wb.add_format({'bold': True, 'bg_color': 'yellow', 'pattern': 1}))

headers_table = ["Product code", "Grouping DV", "Grouping Raw Data"]
for c, h in enumerate(headers_table):
    ws.merge_range(8, c + 1, 9, c + 1, h, wb.add_format({'bold': True, 'bg_color': '#002060', 
                                                  'pattern': 1, 'font_color': 'white', 
                                                  'align': 'center', 'valign': 'center'}))

header_table_notfreezed1 = ["DV Output [1]", "Raw Data [2]", "Checking Results [1]-[2]", "Different Percentage of Checking Result to Raw Data"]
headers_table_notfreezed2 = ["pol_e", "sa_if_m", "anp_if_m", "total_fund_sum"]
header_table_notfreezed1_frm = wb.add_format({'bold': True, 'bg_color': '#002060', 
                                'pattern': 1, 'font_color': 'white', 
                                'align': 'center', 'valign': 'center',
                                'top': 2, 'top_color':'white', 'bottom': 2,
                                'bottom_color': 'white', 'left': 1,'left_color': 'white',
                                'right': 1,'right_color': 'white'})
header_table_notfreezed2_frm = wb.add_format({'bold': True, 'bg_color': '#3A3838', 
                                'pattern': 1, 'font_color': 'white', 
                                'align': 'center', 'valign': 'center',
                                'top': 2, 'top_color':'white', 'bottom': 2,
                                'bottom_color': 'white', 'left': 1,'left_color': 'white',
                                'right': 1,'right_color': 'white'})


for c,h in enumerate(header_table_notfreezed1):
    ws.merge_range(0, 4 * (c + 1) + c, 0, (4 * (c + 1) + c) + 4, h, header_table_notfreezed1_frm)
    ws.merge_range(8, 4 * (c + 1) + c, 8, (4 * (c + 1) + c) + 4, h, header_table_notfreezed1_frm)

for x in range(1, len(header_table_notfreezed1) + 1):
    for c,h in enumerate(headers_table_notfreezed2):
        ws.write(1, c + (4 * (x)), h, header_table_notfreezed2_frm)
        ws.write(9, c + (4 * (x)), h, header_table_notfreezed2_frm)

wb.close()