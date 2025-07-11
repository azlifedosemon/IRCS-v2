import pandas as pd
import numpy as np
import xlsxwriter as xlwrite
import IRCS3_input as input_sheet
import trad_processing 
import time

def elapsed_time(start,end,script):
    if round((end - start),0) > 60:
        print(f"\n• {script} RUNTIME: {round((end - start) / 60, 2)} minutes", end='')
    elif (end - start) < 1:
        print(f"\n• {script} RUNTIME: {round((end - start) * 1000, 2)} ms", end= '')
    else:
        print(f"\n• {script} RUNTIME: {round((end - start), 2)} second", end= '')
    
start_time = time.time()



end_time = time.time()
print("DIAGNOSTICS:", end='')
elapsed_time(input_sheet.start_time, input_sheet.end_time, 'INPUT')
elapsed_time(trad_processing.start_time, trad_processing.end_time, 'TRAD PROCESSING')
elapsed_time(start_time, end_time, "OUTPUT")
elapsed_time(input_sheet.start_time, end_time, "CUMULATIVE")