# Compute total tweets from csv files, check and account for NUL errors
# Alex Norton 2021

import os
import glob
import csv

os.chdir("data_all")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

# Total tweets
total_count = 0

# Open file in read mode
for i, f in enumerate(all_filenames):
    # Per file tweet count
    local_count = 0
    with open(f, 'r') as read_obj:
        # Pass the file object to reader to get the reader object
        csv_reader = csv.reader(read_obj)
        # Iterate over each row in the csv using reader object
        try:
            for row in csv_reader:
                local_count += 1
            # Subtract 1 for header row
            local_count -= 1
            print('Count for file ' + str(i) + ': ' + str(local_count))
        except csv.Error as e:
            print('file %s, line %d: %s' % (f, csv_reader.line_num, e))
    total_count += local_count

# Print total count
print('Count for all files: ' + str(total_count))


