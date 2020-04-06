import shutil
import subprocess
import os

path = os.getcwd()
src = './input/complaints.csv'
dest = path + '/../../input/complaints.csv'
exec_path = path + '/../../src/consumer_complaints.py'

shutil.copy(src, dest)
subprocess.run(['python', exec_path, dest, './output/report.csv'])
