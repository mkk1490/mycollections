import os
import tarfile
from typing import Text
import glob

"""
        extracts the gz and tar files into local

"""

destination_path = "/tmp/extract/"
file_name_pattern = "file_name*"

source_file_list_local = glob.glob(os.path.join(destination_path, file_name_pattern))
for file_name in source_file_list_local:
    if file_name.endswith('tar.gz'):
        tar = tarfile.open(file_name, "r|gz")
        tar.extractall(path=destination_path)
        tar.close()
    elif file_name.endswith(('tar', 'TAR')):
        tar = tarfile.open(file_name, "r:")
        tar.extractall(path=destination_path)
        tar.close()
    else:
        pass
