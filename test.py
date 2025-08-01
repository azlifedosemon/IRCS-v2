Traceback (most recent call last):
  File "d:\Run Control 3\IRCS3_build\IRCS3_program.py", line 4, in <module>
    import IRCS3_input as input_sheet
  File "d:\Run Control 3\IRCS3_build\IRCS3_input.py", line 80, in <module>
    excel_output_trad = get_output_path('Output Trad', 'DV_AZTRAD', PATH_MAP)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "d:\Run Control 3\IRCS3_build\IRCS3_input.py", line 73, in get_output_path
    path_map_df.columns = path_map_df.columns.str.strip()
                          ^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
                   ^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\core\strings\accessor.py", line 181, in __init__
    self._inferred_dtype = self._validate(data)
                           ^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\christo\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages\pandas\core\strings\accessor.py", line 235, in _validate
    raise AttributeError("Can only use .str accessor with string values!")
AttributeError: Can only use .str accessor with string values!