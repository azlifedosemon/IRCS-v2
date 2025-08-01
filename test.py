Usage: python dv_worker.py <path_to_excel> <out_pickle>
Usage: python dv_worker.py <path_to_excel> <out_pickle>
Usage: python dv_worker.py <path_to_excel> <out_pickle>
Usage: python dv_worker.py <path_to_excel> <out_pickle>
Traceback (most recent call last):
  File "d:\Run Control 3\IRCS3_build\IRCS3_program.py", line 5, in <module>
    import trad_processing as trad
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 262, in <module>
    rafm_runs = build_rafm_subprocess(tradfilter)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 200, in build_rafm_subprocess
    run, df = fut.result()
              ^^^^^^^^^^^^
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0\Lib\concurrent\futures\_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0\Lib\concurrent\futures\_base.py", line 401, in __get_result
    raise self._exception
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0\Lib\concurrent\futures\thread.py", line 58, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 183, in run_rafm_worker
    subprocess.check_call([
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.11_3.11.2544.0_x64__qbz5n2kfra8p0\Lib\subprocess.py", line 413, in check_call
    raise CalledProcessError(retcode, cmd)
subprocess.CalledProcessError: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'D:\\Run Control 3\\IRCS3_build\\dv_worker.py', 'run23', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run23TRAD_Con.xlsx']' returned non-zero exit status 1.