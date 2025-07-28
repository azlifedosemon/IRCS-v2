import os
import pandas as pd
import xlsxwriter
import syntax.control_4_trad as trad
import syntax.control_4_ul as ul
import syntax.control_4_reas as reas
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

def process_input_file(file_path):
    filename = os.path.basename(file_path).lower()

    if 'trad' in filename:
        jenis = 'trad'
        result = trad.main({"input excel": file_path})
    elif 'ul' in filename:
        jenis = 'ul'
        result = ul.main({"input excel": file_path})
    elif 'reas' in filename:
        jenis = 'reas'
        result = reas.main({"input excel": file_path})
    else:
        print(f"❌ Jenis file tidak dikenali: {filename}")
        return

    print(f"\n📄 Memproses: {filename} (jenis: {jenis})")

    try:
        df = pd.read_excel(file_path, sheet_name='File Path')
    except Exception as e:
        print(f"⚠️ Tidak bisa membaca sheet 'File Path' dari {file_path}: {e}")
        return

    df.columns = df.columns.str.strip()
    df['Name'] = df['Name'].astype(str).str.strip().str.lower()
    df['File Path'] = df['File Path'].astype(str).str.strip()

    print("Isi 'Name':", df['Name'].tolist())

    if 'output_path' not in df['Name'].values or 'output_filename' not in df['Name'].values:
        print(f"⚠️ output_path atau output_filename tidak ditemukan di sheet 'File Path' pada {file_path}")
        return

    output_path = df.loc[df['Name'] == 'output_path', 'File Path'].values[0]
    output_filename = df.loc[df['Name'] == 'output_filename', 'File Path'].values[0]

    print(f"output_path: {output_path}")
    print(f"output_filename: {output_filename}")

    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, output_filename)

    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for sheet_name, df_sheet in result.items():
            if sheet_name == 'Control':
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
            else:
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, header=True)

            workbook  = writer.book
            worksheet = writer.sheets[sheet_name]

            format_int = workbook.add_format({'num_format': '0'})
            worksheet.set_column(0, df_sheet.shape[1] - 1, None, format_int)

            if sheet_name != 'Control':
                border_format = workbook.add_format({'border': 1, 'border_color': 'black'})
                nrows, ncols = df_sheet.shape
                worksheet.conditional_format(
                    0, 0, nrows, ncols - 1,
                    {'type': 'no_errors', 'format': border_format}
                )

    print(f"✅ Output disimpan di: {output_file}")
    print("📑 Sheet yang diekspor:")
    for sheet in result:
        print(f"   - {sheet}")


def main(input_folder):
    start_time = time.time()

    files = [
        os.path.join(input_folder, fname)
        for fname in os.listdir(input_folder)
        if fname.endswith(".xlsx") and not fname.startswith("~$")
    ]

    if not files:
        print("📂 Tidak ada file .xlsx yang ditemukan di folder input.")
        return

    print(f"🔧 Memproses {len(files)} file secara paralel...\n")

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_input_file, f) for f in files]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Error saat memproses file: {e}")

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n⏲️ Total waktu proses: {duration:.2f} detik")
