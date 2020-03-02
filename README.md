# BE-MESP_data-storer
Part (2/3) of the BE-MESP analyzer suite
Script to manage results of data-collector and store them into a DB
This script takes an alphanumeric string as input subdirectory: a directory with that name will be created with path ../results/1-dc-results/dir_name
It takes an alphanumeric string as output subdirectory: a directory with that name will be created with path ../results/2-ds-results/dir_name
It takes  an alphanumeric string as output db file (OPTIONAL): an output .db file will be created in output directory with tthat name, otherwise a file with Database_random-string.db will be created



Input: > input subdirectory name (in ../results/1-dc-results path)
       > output directory name(in ../results/2-ds-results path)
       > name of the output file (without extention) OPTIONAL

Output > a .db file into the specified output subdirectory
