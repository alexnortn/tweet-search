# Concatenate csv files
# Alex Norton 2021
# Inspired by: https://github.com/ekapope/Combine-CSV-files-in-the-folder/blob/master/Combine_CSVs.py

import os
import glob
import pandas as pd

os.chdir("data_1")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

# Combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
# Export to csv
combined_csv.to_csv("combined_csv.csv", index=False, encoding = 'utf-8-sig')