import shutil
import subprocess
import os

## Before executing this code, a large (moderate) sized csv file like
## The one in the following link should be placed in input with filename complaints.csv
## http://files.consumerfinance.gov/ccdb/complaints.csv.zip
## such a file is not included in this repo in order to not make the repo unnecessarily large 

path = os.getcwd()
src = './input/complaints.csv'
dest = path + '/../../input/complaints.csv'
exec_path = path + '/../../src/consumer_complaints.py'

shutil.copy(src, dest)
subprocess.run(['python', exec_path, dest, './output/report.csv'])
