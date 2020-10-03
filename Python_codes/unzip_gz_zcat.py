import os, glob

import subprocess

source_path = "file_location"
file_name_pattern = "file_name*"

source_file_list_local = glob.glob(os.path.join(source_path, file_name_pattern))
for file_name in source_file_list_local:
    if file_name.endswith('.gz'):
        base_file_name, file_ext = os.path.splitext(file_name)
        print(base_file_name)
    unzip_cmd = "zcat {file_name} > {base_file_name}".format(file_name=file_name, base_file_name=base_file_name)
    print(unzip_cmd)
    unzip_output = subprocess.check_output(['bash', '-c', unzip_cmd])
    print(unzip_output)
