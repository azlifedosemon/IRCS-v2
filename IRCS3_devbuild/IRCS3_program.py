import pandas as pd
import numpy as np
import xlsxwriter as xlwrite
import IRCS3_input as input_sheet
import time

def elapsed_time(start,end):
    if round((end - start),0) > 60:
        print(f"\n RUNTIME: {round((end - start) / 60, 2)} minutes")
    elif (end - start) < 1:
        print(f"\n RUNTIME: {round((end - start) * 1000, 2)} ms")
    else:
        print(f"\n RUNTIME: {round((end - start), 2)} second")
    
start_time = time.time()



end_time = time.time()
runtime = elapsed_time(start_time, end_time)