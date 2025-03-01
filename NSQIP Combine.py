import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'


# Import foundational Python libraries
import random  # For setting the random seed
import pandas as pd  # Main library for reading and manipulating data
from pathlib import Path # Package to handle paths
import torch  # PyTorch package
import numpy as np
import pyarrow as pa  # For use later when saving files as Parquet

pd.options.display.max_rows = 20  # Configure pandas to display up to 20 rows
pd.options.display.max_columns = 0  # Display all columns


# Setting seed for reproducible results
seed = 65

random.seed(seed)

torch.manual_seed(seed)
torch.backends.cudnn.deterministic = True
if torch.cuda.is_available(): torch.cuda.manual_seed_all(seed)

np.random.seed(seed)


# Set paths using pathlib to folders containing the relevant txt files
# All NSQIP txt files except for 2012 and 2019 have similar heading capitalization
# The 2006-2007 files have different race/ethnicity categorization compared to future years so these are excluded
# For NSQIP morbidity and mortality risk calculations, these are only included 2011 onwards
puf_folder = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/NSQIP PUF')
puf12and19_folder = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/NSQIP PUF 12&19')

# Use pathlib's global method to map to all .txt files in the relevant folders
puf_files = list(puf_folder.glob('*.txt'))
puf12and19_files = list(puf12and19_folder.glob('*.txt'))

# Set up a list of variables to import
all_variables = ['CaseID', 'SEX', 'RACE_NEW', 'ETHNICITY_HISPANIC', 'PRNCPTX', 'CPT', 'WORKRVU', 'INOUT', 'TRANST', 'Age', 'AdmYR', 'OperYR', 'DISCHDEST', 'ANESTHES', 'ANESTHES_OTHER', 'SURGSPEC', 'HEIGHT', 'WEIGHT', 'DIABETES', 'SMOKE', 'FNSTATUS2', 'VENTILAT', 'HXCOPD', 'ASCITES', 'HXCHF', 'HYPERMED', 'RENAFAIL', 'DIALYSIS', 'DISCANCR', 'STEROID', 'BLEEDDIS', 'TRANSFUS', 'PRSEPIS', 'DPRNA', 'DPRBUN', 'DPRCREAT', 'DPRALBUM', 'DPRBILI', 'DPRSGOT', 'DPRALKPH', 'DPRWBC', 'DPRHCT', 'DPRPLATE', 'DPRPTT', 'DPRINR', 'DPRHEMO_A1C', 'DPRHEMOGLOBIN', 'PRSODM', 'PRBUN', 'PRCREAT', 'PRALBUM', 'PRBILI', 'PRSGOT', 'PRALKPH', 'PRWBC', 'PRHCT', 'PRPLATE', 'PRPTT', 'PRINR', 'PRHEMO_A1C', 'PRHEMOGLOBIN', 'OTHERCPT1', 'OTHERWRVU1', 'OTHERCPT2', 'OTHERWRVU2', 'OTHERCPT3', 'OTHERWRVU3', 'OTHERCPT4', 'OTHERWRVU4', 'OTHERCPT5', 'OTHERWRVU5', 'OTHERCPT6', 'OTHERWRVU6', 'OTHERCPT7', 'OTHERWRVU7', 'OTHERCPT8', 'OTHERWRVU8', 'OTHERCPT9', 'OTHERWRVU9', 'OTHERCPT10', 'OTHERWRVU10', 'CONCPT1', 'CONWRVU1', 'CONCPT2', 'CONWRVU2', 'CONCPT3', 'CONWRVU3', 'CONCPT4', 'CONWRVU4', 'CONCPT5', 'CONWRVU5', 'CONCPT6', 'CONWRVU6', 'CONCPT7', 'CONWRVU7', 'CONCPT8', 'CONWRVU8', 'CONCPT9', 'CONWRVU9', 'CONCPT10', 'CONWRVU10', 'ASACLAS', 'OPTIME', 'HDISDT', 'YRDEATH', 'TOTHLOS', 'AdmQtr', 'HtoODay', 'SUPINFEC', 'SSSIPATOS', 'WNDINFD', 'DSSIPATOS', 'ORGSPCSSI', 'OSSIPATOS', 'DEHIS', 'OUPNEUMO', 'PNAPATOS', 'REINTUB', 'PULEMBOL', 'DPULEMBOL', 'FAILWEAN', 'VENTPATOS', 'RENAINSF', 'OPRENAFL', 'URNINFEC', 'UTIPATOS', 'CNSCVA', 'CDARREST', 'CDMI', 'NOTHBLEED', 'OTHBLEED', 'OTHDVT', 'DOTHDVT', 'OTHSYSEP', 'SEPSISPATOS', 'OTHSESHOCK', 'SEPSHOCKPATOS', 'PODIAG10', 'RETURNOR', 'DOpertoD', 'DOptoDis', 'STILLINHOSP', 'REOPERATION1', 'RETORPODAYS', 'REOPORCPT1', 'RETORRELATED', 'REOPOR1ICD101', 'REOPERATION2', 'RETOR2PODAYS', 'REOPOR2CPT1', 'RETOR2RELATED', 'REOPOR2ICD101', 'REOPERATION3', 'UNPLANNEDREADMISSION1', 'READMRELATED1', 'READMSUSPREASON1', 'READMRELICD101', 'UNPLANNEDREADMISSION2', 'READMRELATED2', 'READMSUSPREASON2', 'READMRELICD102', 'READMISSION3', 'OTHCDIFF', 'DOTHCDIFF', 'EOL_WDCARE', 'BLEED_UNITS_TOT', 'PREOP_COVID', 'POSTOP_COVID', 'IMMUNO_CAT', 'OXYGEN_SUPPORT', 'CASETYPE', 'HOMESUP', 'HXFALL', 'HXDEMENTIA', 'DELIRIUM', 'DISHOMESVC', 'DISFXNSTAT', 'PREOP_CREAT_MSINCR', 'POSTOP_CREAT_MSINCR', 'OP_APPROACH']

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

# Define a function to read and process the PUF files efficiently as chunks of 100k rows
# Also specify encoding language (NSQIP txt files use latin-1)
def load_puf_files(file_list, dtype_dict, delimiter="\t", encoding="latin-1", chunksize=100000):
    """
    Load large files in chunks and yield dataframes iteratively.
    """
    for file in file_list:
        chunk_iterator = pd.read_csv(file, delimiter=delimiter, dtype=dtype_dict, encoding=encoding, chunksize=chunksize)
        for chunk in chunk_iterator:
            yield chunk

# Process the main PUF files (avoiding full list storage)
combined_puf_df = pd.concat(load_puf_files(puf_files, dtype_dict=types_dict), ignore_index=True)

# Convert column names for 2012 and 2019 files
all_variables_12and19 = [x.upper() for x in all_variables]
int_variables_12and19 = [x.upper() for x in int_variables]
float_variables_12and19 = [x.upper() for x in float_variables]

# Define a dictionary for data types in 2012 and 2019 files
types_dict_12and19 = {col: int for col in int_variables_12and19}
types_dict_12and19.update({col: float for col in float_variables_12and19})
types_dict_12and19.update({col: str for col in all_variables_12and19 if col not in types_dict_12and19})

# Process 2012 and 2019 files efficiently
puf12and19_df = pd.concat(load_puf_files(puf12and19_files, dtype_dict=types_dict_12and19), ignore_index=True)

# Rename columns in 2012/2019 DataFrame
column_mapping = {
    'AGE': 'Age', 'DOPERTOD': 'DOpertoD', 'DOPTODIS': 'DOptoDis',
    'CASEID': 'CaseID', 'OPERYR': 'OperYR', 'HTOODAY': 'HtoODay',
    'ADMYR': 'AdmYR', 'ADMQTR': 'AdmQtr'
}
puf12and19_df.rename(columns=column_mapping, inplace=True)

# Merge both datasets incrementally
nsqip_08to23 = pd.concat([combined_puf_df, puf12and19_df], ignore_index=True)

# Reset the index
nsqip_08to23.reset_index(drop=True, inplace=True)

# Map a path to the folder you want to save to and save as a csv
nsqip_08to23_folder = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23')
nsqip_08to23.to_csv(nsqip_08to23_folder/'nsqip_08to23')



#df_list.set_index('CaseID', inplace=True)  # Set "CaseID" as index

#df_list2.append(pd.read_csv(file, delimiter="\t", usecols=all_variables_12and19, dtype=types_dict_12and19, encoding='latin-1'))




# for file in puf_files:
#     df_list.append(pd.read_csv(file, delimiter="\t", usecols=all_variables, dtype=types_dict, encoding='latin-1'))


