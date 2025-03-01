import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'


# Import foundational Python libraries
import pandas as pd  # Main library for reading and manipulating data
from pathlib import Path # Package to handle paths

# Set paths using pathlib to folders containing the relevant txt files
# The 06-07 files have different race/ethnicity and FNSTATUS categorization so these are separated
puf06to07_folder = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/NSQIP PUF 06&07')
combined_nsqip = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/combined_nsqip')

# Use pathlib's global method to map to all .txt files in the relevant folders
puf06to07_files = list(puf06to07_folder.glob('*.txt'))

# Define output file path
output_file = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/final_merged_nsqip.csv')

# Set up a list of variables to import
all_variables = ['CaseID', 'SEX', 'RACE', 'RACE_NEW', 'ETHNICITY_HISPANIC', 'PRNCPTX', 'CPT', 'WORKRVU', 'INOUT', 'TRANST', 'Age', 'AdmYR', 'OperYR', 'DISCHDEST', 'ANESTHES', 'ANESTHES_OTHER', 'SURGSPEC', 'HEIGHT', 'WEIGHT', 'DIABETES', 'SMOKE', 'FNSTATUS2', 'VENTILAT', 'HXCOPD', 'ASCITES', 'HXCHF', 'HYPERMED', 'RENAFAIL', 'DIALYSIS', 'DISCANCR', 'STEROID', 'BLEEDDIS', 'TRANSFUS', 'PRSEPIS', 'DPRNA', 'DPRBUN', 'DPRCREAT', 'DPRALBUM', 'DPRBILI', 'DPRSGOT', 'DPRALKPH', 'DPRWBC', 'DPRHCT', 'DPRPLATE', 'DPRPTT', 'DPRINR', 'DPRHEMO_A1C', 'DPRHEMOGLOBIN', 'PRSODM', 'PRBUN', 'PRCREAT', 'PRALBUM', 'PRBILI', 'PRSGOT', 'PRALKPH', 'PRWBC', 'PRHCT', 'PRPLATE', 'PRPTT', 'PRINR', 'PRHEMO_A1C', 'PRHEMOGLOBIN', 'OTHERCPT1', 'OTHERWRVU1', 'OTHERCPT2', 'OTHERWRVU2', 'OTHERCPT3', 'OTHERWRVU3', 'OTHERCPT4', 'OTHERWRVU4', 'OTHERCPT5', 'OTHERWRVU5', 'OTHERCPT6', 'OTHERWRVU6', 'OTHERCPT7', 'OTHERWRVU7', 'OTHERCPT8', 'OTHERWRVU8', 'OTHERCPT9', 'OTHERWRVU9', 'OTHERCPT10', 'OTHERWRVU10', 'CONCPT1', 'CONWRVU1', 'CONCPT2', 'CONWRVU2', 'CONCPT3', 'CONWRVU3', 'CONCPT4', 'CONWRVU4', 'CONCPT5', 'CONWRVU5', 'CONCPT6', 'CONWRVU6', 'CONCPT7', 'CONWRVU7', 'CONCPT8', 'CONWRVU8', 'CONCPT9', 'CONWRVU9', 'CONCPT10', 'CONWRVU10', 'ASACLAS', 'OPTIME', 'HDISDT', 'YRDEATH', 'TOTHLOS', 'AdmQtr', 'HtoODay', 'SUPINFEC', 'SSSIPATOS', 'WNDINFD', 'DSSIPATOS', 'ORGSPCSSI', 'OSSIPATOS', 'DEHIS', 'OUPNEUMO', 'PNAPATOS', 'REINTUB', 'PULEMBOL', 'DPULEMBOL', 'FAILWEAN', 'VENTPATOS', 'RENAINSF', 'OPRENAFL', 'URNINFEC', 'UTIPATOS', 'CNSCVA', 'CDARREST', 'CDMI', 'NOTHBLEED', 'OTHBLEED', 'OTHDVT', 'DOTHDVT', 'OTHSYSEP', 'SEPSISPATOS', 'OTHSESHOCK', 'SEPSHOCKPATOS', 'PODIAG10', 'RETURNOR', 'DOpertoD', 'DOptoDis', 'STILLINHOSP', 'REOPERATION1', 'RETORPODAYS', 'REOPORCPT1', 'RETORRELATED', 'REOPOR1ICD101', 'REOPERATION2', 'RETOR2PODAYS', 'REOPOR2CPT1', 'RETOR2RELATED', 'REOPOR2ICD101', 'REOPERATION3', 'UNPLANNEDREADMISSION1', 'READMRELATED1', 'READMSUSPREASON1', 'READMRELICD101', 'UNPLANNEDREADMISSION2', 'READMRELATED2', 'READMSUSPREASON2', 'READMRELICD102', 'READMISSION3', 'OTHCDIFF', 'DOTHCDIFF', 'EOL_WDCARE', 'BLEED_UNITS_TOT', 'PREOP_COVID', 'POSTOP_COVID', 'IMMUNO_CAT', 'OXYGEN_SUPPORT', 'CASETYPE', 'HOMESUP', 'HXFALL', 'HXDEMENTIA', 'DELIRIUM', 'DISHOMESVC', 'DISFXNSTAT', 'PREOP_CREAT_MSINCR', 'POSTOP_CREAT_MSINCR', 'OP_APPROACH']

# Specify variable type, categorizing integer and float variables in two separate lists
# Age is an int variable but won't be specified as this until after cleaned since it also has the value 90+
int_variables = ['CaseID']
float_variables = ['WORKRVU', 'HEIGHT', 'WEIGHT', 'PRSODM', 'PRBUN', 'PRCREAT', 'PRALBUM', 'PRBILI', 'PRSGOT', 'PRALKPH', 'PRWBC', 'PRHCT', 'PRPLATE', 'PRPTT', 'PRINR', 'PRHEMO_A1C', 'PRHEMOGLOBIN', 'OTHERWRVU1', 'OTHERWRVU2', 'OTHERWRVU3', 'OTHERWRVU4', 'OTHERWRVU5', 'OTHERWRVU6', 'OTHERWRVU7', 'OTHERWRVU8', 'OTHERWRVU9', 'OTHERWRVU10', 'CONWRVU1', 'CONWRVU2', 'CONWRVU3', 'CONWRVU4', 'CONWRVU5', 'CONWRVU6', 'CONWRVU7', 'CONWRVU8', 'CONWRVU9', 'CONWRVU10', 'OPTIME', 'AdmYR', 'OperYR', 'DPRNA', 'DPRBUN', 'DPRCREAT', 'DPRALBUM', 'DPRBILI', 'DPRSGOT', 'DPRALKPH', 'DPRWBC', 'DPRHCT', 'DPRPLATE', 'DPRPTT', 'DPRINR', 'DPRHEMO_A1C', 'DPRHEMOGLOBIN', 'HDISDT', 'YRDEATH', 'TOTHLOS', 'HtoODay', 'DPULEMBOL', 'DOTHDVT', 'DOpertoD', 'DOptoDis', 'RETORPODAYS', 'RETOR2PODAYS', 'DOTHCDIFF', 'BLEED_UNITS_TOT']

# Create a dictionary and use this to finish classifying the variable types
types_dict = {}
for i in int_variables:
    types_dict[i] = int
for i in float_variables:
    types_dict[i] = float
types_dict.update({column: str for column in all_variables if column not in types_dict})

# Define mapping for RACE_NEW and ETHNICITY_HISPANIC
race_mapping = {
    'Hispanic, White': ('White', 'Yes'),
    'Hispanic, Black': ('Black or African American', 'Yes'),
    'Hispanic, Color Unknown': ('Unknown/Not Reported', 'Yes'),
    'Black, not of Hispanic Origin': ('Black or African American', 'No'),
    'White, not of Hispanic Origin': ('White', 'No'),
    'American Indian or Alaska Native': ('American Indian or Alaska Native', 'No'),
    'Asian or Pacific Islander': ('Unknown/Not Reported', 'No'),
    'Unknown': ('Unknown/Not Reported', 'Unknown')
}

def map_race_ethnicity(race_value):
    """Maps RACE to RACE_NEW and ETHNICITY_HISPANIC."""
    return race_mapping.get(race_value, ('Unknown/Not Reported', 'Unknown'))

# Load and concatenate puf06to07 files
puf06to07_chunks = []
for file in puf06to07_files:
    chunk = pd.read_csv(file, delimiter="\t", encoding="latin-1")
    if "RACE" in chunk.columns:
        chunk[['RACE_NEW', 'ETHNICITY_HISPANIC']] = chunk['RACE'].apply(lambda x: pd.Series(map_race_ethnicity(x)))
        chunk.drop(columns=['RACE'], inplace=True)
    puf06to07_chunks.append(chunk)

puf06to07_df = pd.concat(puf06to07_chunks, ignore_index=True)

# Get column structure from combined_nsqip
combined_cols = list(pd.read_csv(combined_nsqip, nrows=0).columns)

# Ensure puf06to07_df matches combined_nsqip structure
for col in combined_cols:
    if col not in puf06to07_df.columns:
        puf06to07_df[col] = pd.NA
puf06to07_df = puf06to07_df[combined_cols]

# Write header to output file
pd.DataFrame(columns=combined_cols).to_csv(output_file, index=False)

# Process combined_nsqip in chunks and merge with puf06to07_df only once
chunksize = 100000

# First, write puf06to07_df to the output file
puf06to07_df.to_csv(output_file, index=False)

# Then, process combined_nsqip in chunks and append
for chunk in pd.read_csv(combined_nsqip, encoding='latin-1', chunksize=chunksize):
    chunk.drop_duplicates(subset=['CaseID'], inplace=True)
    chunk.to_csv(output_file, mode='a', header=False, index=False)

print(f"Merged data saved to {output_file}")










# def map_race_ethnicity(race_value):
#     """Maps RACE to RACE_NEW and ETHNICITY_HISPANIC based on predefined mapping."""
#     return race_mapping.get(race_value, ('Unknown/Not Reported', 'Unknown'))
#
# # Define the output file where you want the concatenation to be saved
# output_file = r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/final_merged_nsqip.csv'
#
# # Function to load and process puf06to07_files (tab-delimited) with race/ethnicity mapping
# def load_puf_files(file_list, dtype_dict, encoding="latin-1", chunksize=100000):
#     for file in file_list:
#         chunk_iterator = pd.read_csv(file, delimiter="\t", dtype=dtype_dict, encoding=encoding, chunksize=chunksize)
#
#         for chunk in chunk_iterator:
#             # Apply race/ethnicity mapping ONLY to puf06to07 chunks
#             if "RACE" in chunk.columns:
#                 chunk[['RACE_NEW', 'ETHNICITY_HISPANIC']] = chunk['RACE'].apply(
#                     lambda x: pd.Series(map_race_ethnicity(x)))
#                 chunk.drop(columns=['RACE'], inplace=True)  # Drop redundant column
#
#             chunk.reset_index(drop=True, inplace=True)  # Reset index before merging
#             yield chunk
#
# def merge_in_chunks(file_list_06to07, combined_file, output_file, chunksize=100000):
#     # Read the column structure from combined_nsqip
#     combined_cols = list(pd.read_csv(combined_file, nrows=0).columns)
#
#     # Open output file and write headers only once
#     with open(output_file, 'w', encoding='latin-1') as f_out:
#         pd.DataFrame(columns=combined_cols).to_csv(f_out, index=False)
#
#     # Process combined_nsqip in chunks
#     for combined_chunk in pd.read_csv(combined_file, dtype=types_dict, encoding='latin-1', chunksize=chunksize, delimiter=","):
#         combined_chunk.reset_index(drop=True, inplace=True)
#
#         # Process puf06to07 in chunks
#         for puf_chunk in load_puf_files(file_list_06to07, dtype_dict=types_dict):
#             puf_chunk.reset_index(drop=True, inplace=True)
#
#             # Ensure column consistency between puf_chunk and combined_cols
#             missing_cols = [col for col in combined_cols if col not in puf_chunk.columns]
#             for col in missing_cols:
#                 puf_chunk[col] = pd.NA  # Add missing columns with NaN values
#
#             # Retain only columns from combined_nsqip
#             puf_chunk = puf_chunk[combined_cols]  # Drop any extra columns
#
#             # Merge current chunks
#             merged_chunk = pd.concat([combined_chunk, puf_chunk], ignore_index=True)
#
#             # Drop duplicate CaseID values
#             merged_chunk.drop_duplicates(subset=["CaseID"], inplace=True)
#
#             # Append to the output CSV
#             merged_chunk.to_csv(output_file, mode='a', header=False, index=False)
#
#             # Free memory
#             del merged_chunk, puf_chunk
#
#     print(f"Merged data saved to {output_file}")
#
# # Run the function
# merge_in_chunks(puf06to07_files, combined_nsqip, output_file)







# # Function to load and process puf06to07_files (tab-delimited) with race/ethnicity mapping
# def load_puf_files(file_list, dtype_dict, encoding="latin-1", chunksize=100000):
#     for file in file_list:
#         chunk_iterator = pd.read_csv(file, delimiter="\t", dtype=dtype_dict, encoding=encoding, chunksize=chunksize)
#
#         for chunk in chunk_iterator:
#             chunk.set_index("CaseID", inplace=True)  # Set index as CaseID
#
#             # Apply race/ethnicity mapping ONLY to puf06to07 chunks
#             if "RACE" in chunk.columns:
#                 chunk[['RACE_NEW', 'ETHNICITY_HISPANIC']] = chunk['RACE'].apply(
#                     lambda x: pd.Series(map_race_ethnicity(x)))
#                 chunk.drop(columns=['RACE'], inplace=True)  # Drop redundant column
#
#             yield chunk  # Yield chunk instead of storing in memory
#
#
# # Function to merge chunks and save in piecemeal fashion
# def merge_in_chunks(file_list_06to07, combined_file, output_file):
#     first_chunk = True  # Ensure headers from combined_nsqip
#
#     # Read combined_nsqip in chunks (comma-delimited) WITHOUT race/ethnicity mapping
#     combined_reader = pd.read_csv(combined_file, dtype=types_dict, encoding='latin-1', chunksize=100000, delimiter=",")
#
#     # Process first chunk separately to write headers
#     combined_chunk = next(combined_reader)  # Read first chunk
#     combined_chunk.set_index("CaseID", inplace=True)
#
#     # Write first chunk with headers
#     combined_chunk.to_csv(output_file, mode='w', header=True, index=True)
#     first_chunk = False  # Prevent headers from being rewritten
#
#     del combined_chunk  # Free memory
#
#     # Process remaining chunks and append
#     for combined_chunk in combined_reader:
#         combined_chunk.set_index("CaseID", inplace=True)  # Set index
#
#         for puf06to07_chunk in load_puf_files(file_list_06to07, dtype_dict=types_dict):
#             puf06to07_chunk.set_index("CaseID", inplace=True)  # Set index
#
#             # Merge both chunks
#             merged_chunk = pd.merge(combined_chunk, puf06to07_chunk, on="CaseID", how="outer")
#             merged_chunk.drop_duplicates(subset=["CaseID"], inplace=True)  # Remove duplicates
#
#             # Append merged chunk to CSV (no headers)
#             merged_chunk.to_csv(output_file, mode='a', header=False, index=True)
#
#             # Free memory
#             del merged_chunk, puf06to07_chunk
#
#     print(f"Merged data saved to {output_file}")
#
#
# # Perform merge and save directly to disk
# output_file = r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/final_merged_nsqip.csv'
# merge_in_chunks(puf06to07_files, combined_nsqip, output_file)


# # Define a function to read and process the PUF 06 and 07 files efficiently as chunks of 100k rows
# # Also specify encoding language (NSQIP txt files use latin-1)
# def load_puf_files(file_list, dtype_dict, delimiter="\t", encoding="latin-1", chunksize=100000):
#     """
#     Load large files in chunks and yield dataframes iteratively.
#     """
#     for file in file_list:
#         chunk_iterator = pd.read_csv(file, delimiter=delimiter, dtype=dtype_dict, encoding=encoding, chunksize=chunksize)
#
#         chunk.set_index("CaseID", inplace=True)  # Set index as CaseID for efficient merge
#
#         # Map the 'RACE' column to two new columns 'RACE_NEW' and 'ETHNICITY_HISPANIC'
#         chunk[['RACE_NEW', 'ETHNICITY_HISPANIC']] = chunk['RACE'].apply(
#             lambda x: pd.Series(race_mapping.get(x, ('Unknown/Not Reported', 'Unknown'))))
#
#         # Drop the redundant "RACE" column from memory
#         chunk.drop(columns=['RACE'], inplace=True)
#
#         for chunk in chunk_iterator:
#             yield chunk
#
# # Process and combine the 06 and 07 PUF files (avoiding full list storage)
# puf06to07_df = pd.concat(load_puf_files(puf06to07_files, dtype_dict=types_dict), ignore_index=True)
#
# # Read the combined, large PUF file
# combined_puf_df = pd.read_csv(combined_nsqip, dtype=types_dict, encoding='latin-1', chunksize=100000)
#
# # Merge both datasets incrementally
# nsqip = pd.concat([combined_puf_df, puf06to07_df], ignore_index=True)

# Write the final combined dataframe 'nsqip' to CSV incrementally



# # Define a function to read and process the combined PUF file efficiently as chunks of 100k rows
# def load_combined_file(files, dtype_dict, delimiter="\t", encoding="latin-1", chunksize=100000):
#     """
#     Load large files in chunks and yield dataframes iteratively.
#     """
#     for file in files:
#         chunk_iterator = pd.read_csv(file, delimiter=delimiter, dtype=dtype_dict, encoding=encoding, chunksize=chunksize)
#         for chunk in chunk_iterator:
#             yield chunk