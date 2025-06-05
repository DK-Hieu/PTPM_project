import subprocess
import shutil
import sys
import os

# Luồng update dữ liệu nguồn CSV
files_csv_to_database = [
                            'stg_csv_links.py'
                            ,'stg_csv_keywords.py'
                            ,'stg_csv_user.py'
                            ,'stg_csv_ratings.py' # file nặng
                        ]


# luồng update dữ liệu raw từ TMDB vào Datalake Lưu ý: crawl số lượng lớn rất lâu
files_rawsoruce_to_datalake = [
                                'stg_csv_links.py'
                                ,'stg_tmdb_json_movie_metadata.py'
                                ,'stg_tmdb_json_movie_credits.py'
                                ,'stg_tmdb_cast.py'
                                ,'stg_tmdb_crew.py' 
                                ,'stg_tmdb_json_person.py' # rất lâu
                            ]

# Luồng update dữ liệu từ Datalake vào Database
files_datalake_to_database = [
                                'stg_tmdb_movie_metadata.py'
                                ,'stg_tmdb_collection.py'
                                ,'stg_tmdb_genres.py'
                                ,'stg_tmdb_person.py'
                            ]

# Luồng tổng hợp
files_to_run_all = [
                    'stg_csv_links.py'
                    ,'stg_csv_keywords.py'
                    ,'stg_csv_user.py'
                    ,'stg_csv_ratings.py'
                    ,'stg_tmdb_json_movie_metadata.py'
                    ,'stg_tmdb_json_movie_credits.py'
                    ,'stg_tmdb_movie_metadata.py'
                    ,'stg_tmdb_collection.py'
                    ,'stg_tmdb_genres.py'
                    ,'stg_tmdb_cast.py'
                    ,'stg_tmdb_crew.py' 
                    ,'stg_tmdb_json_person.py'
                    ,'stg_tmdb_person.py'
                ]

# files_to_run = ['stg_csv_ratings.py']

# data_folder = "output"  # Đây là nơi chứa dữ liệu được tạo bởi các file

# def delete_output_data():
#     if os.path.exists(data_folder):
#         print(f"Lỗi xảy ra. Đang xoá dữ liệu tại '{data_folder}'...")
#         shutil.rmtree(data_folder)
#         print("Đã xoá dữ liệu.")
#     else:
#         print("Không tìm thấy dữ liệu để xoá.")

python_env_path = r"C:\Users\LENOVO\Envs\DK-local\Scripts\python.exe"

files_to_run = files_datalake_to_database

for file in files_to_run:
    print(f"\n▶️ Đang chạy {file}...")
    try:
        result = subprocess.run(
            [python_env_path, file],
            check=True,
            # capture_output=True,
            text=True,
            cwd= r'F:\Work\Caohoc_2024_2026\PTPM_project\PTPM\prod'
        )
        print(f"✅ {file} chạy thành công.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy {file}:")
        print(f">>> STDOUT:\n{e.stdout}")
        print(f">>> STDERR:\n{e.stderr}")
        sys.exit(1)
