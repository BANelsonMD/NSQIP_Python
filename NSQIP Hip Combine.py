import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'


# Import foundational Python libraries
import pandas as pd  # Main library for reading and manipulating data
from pathlib import Path # Package to handle paths

# Set path to using pathlib to folders containing the relevant txt files
# All targeted hip txt files have the CaseID column heading capitalized to "CASEID", just FYI
# I saved the txt files to Excel, reformatted and combined them there so that isn't included here
hip_combined = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/NSQIP Hip/hip_combined.txt')
nsqip08to23 = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/nsqip_08to23')

# Also define the path to the output folder and the output file name once combined
combined_folder = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23')
output_file = combined_folder / "combined_nsqip"

# Define a function to read and process a single file efficiently in chunks of 100k rows
# Also specify encoding language (NSQIP txt files use latin-1)
def load_hip_file(file_path, dtype_dict, delimiter="\t", encoding="latin-1", chunksize=100000):
    """
    Load a large file in chunks and yield dataframes iteratively.
    """
    chunk_iterator = pd.read_csv(file_path, delimiter=delimiter, dtype=dtype_dict, encoding=encoding, chunksize=chunksize)
    for chunk in chunk_iterator:
        yield chunk

# Call the function and process the hip_combined file
hip_combined_df = pd.concat(load_hip_file(hip_combined, dtype_dict=str), ignore_index=True)

# Set CaseID as index for efficient merge with larger df
hip_combined_df.set_index("CaseID", inplace=True)

# Read combined PUF file in chunks (note that the combined NSQIP file uses commas, not tabs to separate columns)
chunk_size = 100000  # Adjust based on available memory
chunks = pd.read_csv(nsqip08to23, encoding="latin-1", chunksize=chunk_size)

# Write header first
first_chunk = True

# Process hip files and merge on CaseID in the loop
for chunk in chunks:
    chunk.set_index("CaseID", inplace=True)  # Set index as CaseID for efficient merge

    # Merge using pd.merge to handle multiple rows with the same CaseID
    merged_chunk = pd.merge(chunk, hip_combined_df, how="left", left_index=True, right_index=True)

    # Write to CSV incrementally
    merged_chunk.to_csv(output_file, mode="w" if first_chunk else "a", header=first_chunk)
    first_chunk = False  # Ensure header is only written once

    del merged_chunk  # Free up memory

# Final cleanup
del hip_combined_df  # Remove reference to hip data to free up memory
