import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'


# Import foundational Python libraries
import pandas as pd  # Main library for reading and manipulating data
from pathlib import Path # Package to handle paths

# Set path to the combined PUF txt file
nsqip = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/final_merged_nsqip.csv')

# Open the file and print the first 10 lines without reading the entire file
with open(nsqip, 'r') as file:
    for i, line in enumerate(file):
        if i < 10:  # Print first 10 lines
            print(line.strip())  # Remove extra newline characters
        else:
            break

# # Initialize a variable to store total duplicate count
# total_duplicate_count = 0
#
# # Assume you're reading from a CSV or a large DataFrame
# chunksize = 100000
# for chunk in pd.read_csv(nsqip, chunksize=chunksize):
#     # Set the 'CaseID' column as the index if it's not already
#     chunk.set_index('CaseID', inplace=True)
#
#     # Find duplicates based on the index 'CaseID'
#     duplicates = chunk[chunk.index.duplicated()]
#
#     # Count the number of duplicate rows in this chunk
#     total_duplicate_count += duplicates.shape[0]
#
# print(f"Total number of duplicate rows: {total_duplicate_count}")