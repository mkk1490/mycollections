import glob, os
import subprocess

file_glob_header_dict = {'glob_pattern': 'source_file*', 'header_string': 'col1,col2,col3'}
source_path = '/location/'


def normalize(inp_str):
    return inp_str.replace(' ', '').replace('\n', '').lower()


def get_first_line(full_file_path):
    with open(full_file_path) as f:
        return f.readline()


# get the 'ACTUAL_HEADER' from the source file
# send the 'EXPECTED HEADER' as an input string along with the source file delimiter
# eg. EXPECTED_HEADER: 'col1,col2,col3'

def gen_error_msg(exp_file_header, actual_file_header):
    return {"EXPECTED_HEADER": exp_file_header, "ACTUAL_HEADER": actual_file_header}


# glob_pattern is file pattern. If the file name is source_file.csv, then glob_pattern will be "source_file*"

for glob_pattern, header_string in file_glob_header_dict.items():
    print(header_string)
    path = source_path + glob_pattern
    source_file_list = glob.glob(path)
    if len(source_file_list) == 0:
        raise FileNotFoundError
    for source_file in source_file_list:
        expected_header = normalize(header_string)
        actual_header = normalize(get_first_line(source_file))
        if actual_header != expected_header:
            print("header is not present")
            sed_command = 'sed -i.bak ' + "'1s/.*/" + header_string + "/' " + source_file
            code = subprocess.check_output(['bash', '-c', sed_command])

