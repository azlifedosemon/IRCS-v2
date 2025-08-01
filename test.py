PS D:\Run Control 3> & C:\Users\christo\AppData\Local\Microsoft\WindowsApps\python3.11.exe "d:/Run Control 3/IRCS3_build/IRCS3_program.py"
❌ Error in RAFM subprocess for run142: expected str, bytes or os.PathLike object, not NoneType
❌ Error in RAFM subprocess for run142: expected str, bytes or os.PathLike object, not NoneType
❌ Error in RAFM subprocess for run142: expected str, bytes or os.PathLike object, not NoneType
❌ Error in RAFM subprocess for run142: expected str, bytes or os.PathLike object, not NoneType
Traceback (most recent call last):
  File "d:\Run Control 3\IRCS3_build\IRCS3_program.py", line 5, in <module>
    import trad_processing as trad
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 303, in <module>
    rafm_df = rafm_runs[run_name]
              ~~~~~~~~~^^^^^^^^^^
KeyError: 'run4'