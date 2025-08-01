Traceback (most recent call last):
  File "d:\Run Control 3\IRCS3_build\IRCS3_program.py", line 5, in <module>
    import trad_processing as trad
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 280, in <module>
    rafm_runs = build_rafm_subprocess(tradfilter)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 218, in build_rafm_subprocess
    run, df = fut.result()
    ^^^^^^^
TypeError: cannot unpack non-iterable NoneType object