Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run4'
Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run13'
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run23'
Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run142'
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
subprocess.CalledProcessError: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'D:\\Run Control 3\\IRCS3_build\\dv_worker.py', 'run4', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run4TRAD_Con.xlsx']' returned non-zero exit status 1.
PS D:\Run Control 3> & C:\Users\christo\AppData\Local\Microsoft\WindowsApps\python3.11.exe "d:/Run Control 3/IRCS3_build/IRCS3_program.py"
Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
Traceback (most recent call last):
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
FileNotFoundError: [Errno 2] No such file or directory: 'run4'
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run13'
Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run23'
Traceback (most recent call last):
  File "D:\Run Control 3\IRCS3_build\dv_worker.py", line 16, in <module>
    df = pd.read_excel(path, engine='openpyxl')
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 478, in read_excel
    io = ExcelFile(io, storage_options=storage_options, engine=engine)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 1513, in __init__
    self._reader = self._engines[engine](self._io, storage_options=storage_options)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_openpyxl.py", line 549, in __init__
    super().__init__(filepath_or_buffer, storage_options=storage_options)
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\excel\_base.py", line 530, in __init__
    self.handles = get_handle(
                   ^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\io\common.py", line 868, in get_handle
    handle = open(handle, ioargs.mode)
             ^^^^^^^^^^^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'run142'
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
subprocess.CalledProcessError: Command '['C:\\Users\\christo\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\\python.exe', 'D:\\Run Control 3\\IRCS3_build\\dv_worker.py', 'run4', 'D:\\Run Control 3\\Source\\Trad\\Data_Extraction_run4TRAD_Con.xlsx']' returned non-zero exit status 1.