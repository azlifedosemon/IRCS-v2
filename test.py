❌ File not found: d:\Run Control 3\IRCS3_build\rafm_run13.pkl
❌ File not found: d:\Run Control 3\IRCS3_build\rafm_run4.pkl
❌ File not found: d:\Run Control 3\IRCS3_build\rafm_run23.pkl
❌ Error in RAFM worker for run run13: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'd:\\Run Control 3\\IRCS3_build\\rafm_worker.py', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run13TRAD_Con.xlsx', 'd:\\Run Control 3\\IRCS3_build\\rafm_run13.pkl']' returned non-zero exit status 1.
❌ Error in RAFM worker for run run4: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'd:\\Run Control 3\\IRCS3_build\\rafm_worker.py', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run4TRAD_Con.xlsx', 'd:\\Run Control 3\\IRCS3_build\\rafm_run4.pkl']' returned non-zero exit status 1.
❌ Error in RAFM worker for run run23: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'd:\\Run Control 3\\IRCS3_build\\rafm_worker.py', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run23TRAD_Con.xlsx', 'd:\\Run Control 3\\IRCS3_build\\rafm_run23.pkl']' returned non-zero exit status 1.
❌ File not found: d:\Run Control 3\IRCS3_build\rafm_run142.pkl
❌ Error in RAFM worker for run run142: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'd:\\Run Control 3\\IRCS3_build\\rafm_worker.py', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run142TRAD_Con.xlsx', 'd:\\Run Control 3\\IRCS3_build\\rafm_run142.pkl']' returned non-zero exit status 1.
Traceback (most recent call last):
  File "d:\Run Control 3\IRCS3_build\IRCS3_program.py", line 5, in <module>
    import trad_processing as trad
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 274, in <module>
    rafm_runs = build_rafm_subprocess(tradfilter)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 212, in build_rafm_subprocess
    run, df = fut.result()
    ^^^^^^^
TypeError: cannot unpack non-iterable NoneType object