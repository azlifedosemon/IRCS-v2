Traceback (most recent call last):
  File "d:\Run Control 3\IRCS3_build\IRCS3_program.py", line 5, in <module>
    import trad_processing as trad
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 281, in <module>
    rafm_runs = build_rafm_subprocess(tradfilter)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "d:\Run Control 3\IRCS3_build\trad_processing.py", line 207, in build_rafm_subprocess
    for i, row in tradfilter.iterrows():
                  ^^^^^^^^^^^^^^^^^^^
AttributeError: 'dict' object has no attribute 'iterrows'