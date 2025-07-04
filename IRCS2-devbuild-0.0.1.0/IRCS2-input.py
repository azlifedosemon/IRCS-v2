# Input Script

# 1) copy paste the file path, make sure the 
#    files are in the same folder with this python program
#    copy the path inside the ""

# DV AZTRAD (CSV)
DV_AZTRAD_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\DV_AZTRAD_Stat.csv"

# DV AZUL (CSV)
DV_AZUL_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\DV_AZUL_Stat.csv"

# IT AZTRAD (CSV)
IT_AZTRAD_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\IT_AZTRAD_FULL_Stat.csv"

# IT AZUL (CSV)
IT_AZUL_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\IT_AZUL_FULL_Stat.csv"

#CODE LIBRARY
CODE_LIBRARY_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\CODE LIBRARY.xlsx"

# 2) enter the intended file name for the output file

#OUTPUT PATH
xlsx_filename = "test_file" #enter the intended file name without .xlsx
xlsx_output = "/".join([x for x in DV_AZTRAD_path.split('\\')][:len(DV_AZTRAD_path.split('\\')) - 1 ]) + "/" + xlsx_filename + ".xlsx"

user_input = [DV_AZTRAD_path, DV_AZUL_path, IT_AZTRAD_path, IT_AZUL_path, CODE_LIBRARY_path, xlsx_output]

# ARCHIVE IN CASE OF ACCIDENTAL DELETION
# *to use archive:
# 1) copy paste to code above and remove one # at the start of each line
# 2) or copy paste to code above, select all the pasted archive, and press ctrl+/
# 3) alternatively read: 
#    https://www.geeksforgeeks.org/python/how-to-comment-out-a-block-of-code-in-python/

# # [ARCHIVE] Input Script

# # 1) copy paste the file path, make sure the 
# #    files are in the same folder with this python program
# #    copy the path inside the ""

# # DV AZTRAD (CSV)
# DV_AZTRAD_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\DV_AZTRAD_Stat.csv"

# # DV AZUL (CSV)
# DV_AZUL_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\DV_AZUL_Stat.csv"

# # IT AZTRAD (CSV)
# IT_AZTRAD_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\IT_AZTRAD_FULL_Stat.csv"

# # IT AZUL (CSV)
# IT_AZUL_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\IT_AZUL_FULL_Stat.csv"

# #CODE LIBRARY
# CODE_LIBRARY_path = r"D:\1. IRCS Automation\Control 2 DEV\IRCS\Control 2\CODE LIBRARY.xlsx"

# # 2) enter the intended file name for the output file

# #OUTPUT PATH
# xlsx_filename = "test_file" #enter the intended file name without .xlsx
# xlsx_output = "/".join([x for x in DV_AZTRAD_path.split('\\')][:len(DV_AZTRAD_path.split('\\')) - 1 ]) + "/" + xlsx_filename + ".xlsx"

# user_input = [DV_AZTRAD_path, DV_AZUL_path, IT_AZTRAD_path, IT_AZUL_path, CODE_LIBRARY_path, xlsx_output]