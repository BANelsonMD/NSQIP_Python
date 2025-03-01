import os
os.environ['KMP_DUPLICATE_LIB_OK']='True'

# Import foundational Python libraries
import pandas as pd  # Main library for reading and manipulating data
from pathlib import Path # Package to handle paths
import numpy as np
import pyarrow as pa

pd.options.display.max_rows = 20  # Configure pandas to display up to 20 rows
pd.options.display.max_columns = 0  # Display all columns

# Set path using pathlib to relevant txt file
# For NSQIP morbidity and mortality risk calculations, these are only included 2011 onwards
# The morb and mort columns as well as a few other columns have varying capitalizations that need to be addressed
nsqip = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23/final_merged_nsqip.csv')

# Set up a list of variables to import, including the ones with varying capitalizations
# Examine this list and cross reference to the NSQIP Data Dictionary to verify they suit your purposes
#Remember to add variables added from new boolean columns
all_variables = [
    'CaseID', 'SEX', 'RACE_NEW', 'ETHNICITY_HISPANIC', 'PRNCPTX', 'CPT', 'WORKRVU', 'INOUT', 'TRANST', 'Age', 'Age_NEW',
    'AdmYR', 'OperYR', 'DISCHDEST', 'ANESTHES', 'ANESTHES_OTHER', 'SURGSPEC', 'HEIGHT', 'WEIGHT', 'DIABETES', 'SMOKE',
    'FNSTATUS2', 'VENTILAT', 'HXCOPD', 'ASCITES', 'HXCHF', 'HYPERMED', 'RENAFAIL', 'DIALYSIS', 'DISCANCR', 'STEROID',
    'BLEEDDIS', 'TRANSFUS', 'PRSEPIS', 'DPRNA', 'DPRBUN', 'DPRCREAT', 'DPRALBUM', 'DPRBILI', 'DPRSGOT', 'DPRALKPH',
    'DPRWBC', 'DPRHCT', 'DPRPLATE', 'DPRPTT', 'DPRINR', 'DPRHEMO_A1C', 'DPRHEMOGLOBIN', 'PRSODM', 'PRBUN', 'PRCREAT',
    'PRALBUM', 'PRBILI', 'PRSGOT', 'PRALKPH', 'PRWBC', 'PRHCT', 'PRPLATE', 'PRPTT', 'PRINR', 'PRHEMO_A1C',
    'PRHEMOGLOBIN', 'OTHERCPT1', 'OTHERWRVU1', 'OTHERCPT2', 'OTHERWRVU2', 'OTHERCPT3', 'OTHERWRVU3', 'OTHERCPT4', 'OTHERWRVU4',
    'OTHERCPT5', 'OTHERWRVU5', 'OTHERCPT6', 'OTHERWRVU6', 'OTHERCPT7', 'OTHERWRVU7', 'OTHERCPT8', 'OTHERWRVU8', 'OTHERCPT9',
    'OTHERWRVU9', 'OTHERCPT10', 'OTHERWRVU10', 'CONCPT1', 'CONWRVU1', 'CONCPT2', 'CONWRVU2', 'CONCPT3', 'CONWRVU3',
    'CONCPT4', 'CONWRVU4', 'CONCPT5', 'CONWRVU5', 'CONCPT6', 'CONWRVU6', 'CONCPT7', 'CONWRVU7', 'CONCPT8', 'CONWRVU8',
    'CONCPT9', 'CONWRVU9', 'CONCPT10', 'CONWRVU10', 'ASACLAS', 'MORTPROB', 'MORBPROB', 'OPTIME', 'HDISDT', 'YRDEATH',
    'TOTHLOS', 'AdmQtr', 'HtoODay', 'SUPINFEC', 'SSSIPATOS', 'WNDINFD', 'DSSIPATOS', 'ORGSPCSSI', 'OSSIPATOS', 'DEHIS',
    'OUPNEUMO', 'PNAPATOS', 'REINTUB', 'PULEMBOL', 'DPULEMBOL', 'FAILWEAN', 'VENTPATOS', 'RENAINSF', 'OPRENAFL',
    'URNINFEC', 'UTIPATOS', 'CNSCVA', 'CDARREST', 'CDMI', 'NOTHBLEED', 'OTHBLEED', 'OTHDVT', 'DOTHDVT', 'OTHSYSEP',
    'SEPSISPATOS', 'OTHSESHOCK', 'SEPSHOCKPATOS', 'PODIAG', 'PODIAG10', 'RETURNOR', 'DOpertoD', 'DOptoDis',
    'STILLINHOSP', 'REOPERATION1', 'RETORPODAYS', 'REOPORCPT1', 'RETORRELATED', 'REOPOR1ICD101', 'REOPERATION2', 'RETOR2PODAYS',
    'REOPOR2CPT1', 'RETOR2RELATED', 'REOPOR2ICD101', 'REOPERATION3', 'UNPLANNEDREADMISSION1', 'READMRELATED1',
    'READMSUSPREASON1', 'READMRELICD101', 'UNPLANNEDREADMISSION2', 'READMRELATED2', 'READMSUSPREASON2',
    'READMRELICD102', 'READMISSION3', 'OTHCDIFF', 'DOTHCDIFF', 'EOL_WDCARE', 'BLEED_UNITS_TOT', 'PREOP_COVID', 'POSTOP_COVID',
    'IMMUNO_CAT', 'OXYGEN_SUPPORT', 'CASETYPE', 'HOMESUP', 'HXFALL', 'HXDEMENTIA', 'DELIRIUM', 'DISHOMESVC', 'DISFXNSTAT',
    'PREOP_CREAT_MSINCR', 'POSTOP_CREAT_MSINCR', 'OP_APPROACH', 'ROBOT_USED', 'REOPERATION', 'UNPLANREADMISSION',
    'MortProb', 'MorbProb', 'MORTPROB', 'MORBPROB', 'HIP_PREOP_DEMENTIA', 'HIP_PREOP_DELIRIUM', 'HIP_PREOP_BONEMEDS',
    'HIP_PREOP_MOBAID', 'HIP_PREOP_PRESSORE', 'HIP_MED_COMGMT', 'HIP_STDCARE', 'HIP_WBAT_POD1', 'HIP_DVT_28D',
    'HIP_FRACTYPE', 'HIP_PATHFRAC', 'HIP_POST_PRESSORE', 'HIP_POST_DELIRIUM', 'HIP_POST_MOBAID', 'HIP_POST_BONEMEDS',
    'HISPANIC', 'INPATIENT', 'SMOKER', 'SSSI', 'DSSI', 'OSSI', 'WOUND', 'PNEUMONIA', 'UNPLANNED_INT', 'PE', 'VENT48',
    'PRI', 'ARF', 'UTI', 'STROKE', 'CARD_ARREST_CPR', 'MI', 'DVT1', 'DVT2', 'SEPSIS', 'SEPSHOCK', 'TRANSFUSION',
    'DECEASED', 'CDIFF', 'POSTOP_DELIRIUM', 'DC_SERVICES'
]

# Specify variable type, categorizing integer and float variables in two separate lists
# Age is an int variable but won't be specified as this until after cleaned since it also has the value 90+
# Most other variables will be classified as floats instead of int since many have missing values
int_variables = ['CaseID']
float_variables = ['WORKRVU', 'HEIGHT', 'WEIGHT', 'PRSODM', 'PRBUN', 'PRCREAT', 'PRALBUM', 'PRBILI', 'PRSGOT',
                   'PRALKPH', 'PRWBC', 'PRHCT', 'PRPLATE', 'PRPTT', 'PRINR', 'PRHEMO_A1C', 'PRHEMOGLOBIN', 'OTHERWRVU1',
                   'OTHERWRVU2', 'OTHERWRVU3', 'OTHERWRVU4', 'OTHERWRVU5', 'OTHERWRVU6', 'OTHERWRVU7', 'OTHERWRVU8',
                   'OTHERWRVU9', 'OTHERWRVU10', 'CONWRVU1', 'CONWRVU2', 'CONWRVU3', 'CONWRVU4', 'CONWRVU5', 'CONWRVU6',
                   'CONWRVU7', 'CONWRVU8', 'CONWRVU9', 'CONWRVU10', 'OPTIME', 'AdmYR', 'OperYR', 'DPRNA', 'DPRBUN',
                   'DPRCREAT', 'DPRALBUM', 'DPRBILI', 'DPRSGOT', 'DPRALKPH', 'DPRWBC', 'DPRHCT', 'DPRPLATE', 'DPRPTT',
                   'DPRINR', 'DPRHEMO_A1C', 'DPRHEMOGLOBIN', 'HDISDT', 'YRDEATH', 'TOTHLOS', 'HtoODay', 'DPULEMBOL',
                   'DOTHDVT', 'DOpertoD', 'DOptoDis', 'RETORPODAYS', 'RETOR2PODAYS', 'DOTHCDIFF', 'BLEED_UNITS_TOT',
                   'MortProb', 'MorbProb', 'MORTPROB', 'MORBPROB']

# Ordinal category mappings
ordinal_mappings = {
    'DIABETES': ('NO', 'NON-INSULIN', 'INSULIN'),
    'FNSTATUS2': ('Independent', 'Partially Dependent', 'Totally Dependent'),
    'PRSEPIS': ('None', 'SIRS', 'Sepsis', 'Septic Shock'),
    'DYSPNEA': ('No', 'MODERATE EXERTION', 'AT REST'),
    'WNDCLAS': ('1-Clean', '2-Clean/Contaminated', '3-Contaminated', '4-Dirty/Infected'),
    'HIP_RES30D': ('Home', 'Separate acute care', 'Unskilled facility', 'Skilled care', 'Facility which was home', 'Still in hospital', 'Expired'),
    'HIP_MED_COMGMT': ('No', 'Yes-partial co-management during stay', 'Yes-co-management throughout stay'),
    'TRANST': ('Home/Permanent residence', 'Acute care hospital', 'Other facility'),
    'DISCHDEST': ('Home/Permanent residence', 'Acute care hospital', 'Other facility', 'Against Medical Advice (AMA)', 'Expired'),
    'AdmQtr': ('1', '2', '3', '4'),
    'IMMUNO_CAT': ('Corticosteroids', 'Anti-rejection/transplant immunosuppressants', 'Other'),
    'CASETYPE': ('Elective', 'Urgent', 'Emergent'),
    'HOMESUP': ('Lives alone at home', 'Lives at home with other individuals'),
    'DISFXNSTAT': ('Independent', 'Partially Dependent', 'Totally Dependent', 'Expired'),
    'PREOP_CREAT_MSINCR': ('Increase in SCr of >=0.3 mg/dL to >=4.0 mg/dL within 48 hours', 'Increase in SCr to >=1.5 times baseline to >=4.0 mg/dL within 7 days', 'Increase in SCr to >=3.0 times baseline within 7 days', 'Increase of 2.0 to <3.0 times baseline within 7 days'),
    'POSTOP_CREAT_MSINCR': ('Increase in SCr of >=0.3 mg/dL to >=4.0 mg/dL within 48 hours', 'Increase in SCr to >=1.5 times baseline to >=4.0 mg/dL within 7 days', 'Increase in SCr to >=3.0 times baseline within 7 days', 'Increase of 2.0 to <3.0 times baseline within 7 days'),
    'ASACLAS': ('1-No Disturb', '2-Mild Disturb', '3-Severe Disturb', '4-Life Threat', '5-Moribund'),
}

# Create a dictionary and use this to finish specifying variable types
types_dict = {}
for i in int_variables:
    types_dict[i] = int
for i in float_variables:
    types_dict[i] = float
for i in ordinal_mappings:
    types_dict[i] = 'category'
types_dict.update({column: str for column in all_variables if column not in types_dict})

# Define the columns we are planning to drop in the process_chunk function
columns_to_drop = ['REOPERATION', 'UNPLANREADMISSION', 'MortProb', 'MorbProb', 'HEIGHT', 'WEIGHT', 'INOUT', 'SMOKE', 'SUPINFEC',
                   'SSSIPATOS', 'WNDINFD', 'DSSIPATOS', 'ORGSPCSSI', 'OSSIPATOS', 'DEHIS', 'OUPNEUMO', 'PNAPATOS',
                   'REINTUB', 'PULEMBOL', 'DPULEMBOL', 'FAILWEAN', 'VENTPATOS', 'RENAINSF', 'OPRENAFL', 'URNINFEC',
                   'UTIPATOS', 'CNSCVA', 'CDARREST', 'CDMI', 'OTHBLEED', 'OTHDVT', 'OTHSYSEP',
                   'SEPSISPATOS', 'OTHSESHOCK', 'SEPSHOCKPATOS', 'RETURNOR', 'DELIRIUM', 'OTHCDIFF', 'DISHOMESVC']

# Function to process each chunk of the combined NSQIP file
def process_chunk(chunk):
    """Cleans and processes a chunk of the dataset."""
    # Replace '90+' in Age column and convert it to integer
    chunk['Age_NEW'] = chunk['Age'].replace('90+', '90').astype(int)

    # Replace non-matching responses to columns to match across years
    chunk['DIABETES'] = chunk['DIABETES'].map({'NO': 'No', 'NON-INSULIN': 'Non-insulin', 'Oral': 'Non-insulin', 'INSULIN': 'Insulin'})

    # Generate multi-label condition variables (Boolean variables categorized based on multiple conditions)
    # Use pandas vectorized operations for efficiency
    # Note the 2007-08 tables don't include PATOS Yes/No, so these will be listed as NaN in new columns
    # We will deal with any missing values using MissForest imputation where appropriate in the future
    chunk['HISPANIC'] = chunk['ETHNICITY_HISPANIC'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['INPATIENT'] = chunk['INOUT'] == 'Inpatient'
    chunk['SMOKER'] = chunk['SMOKE'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['VENTILAT'] = chunk['VENTILAT'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['HXCOPD'] = chunk['HXCOPD'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['ASCITES'] = chunk['ASCITES'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['HXCHF'] = chunk['HXCHF'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['HYPERMED'] = chunk['HYPERMED'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['RENAFAIL'] = chunk['RENAFAIL'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['DIALYSIS'] = chunk['DIALYSIS'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['DISCANCR'] = chunk['DISCANCR'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['STEROID'] = chunk['STEROID'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['BLEEDDIS'] = chunk['BLEEDDIS'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['TRANSFUS'] = chunk['TRANSFUS'].map({'Yes': True, 'No': False, 'Unknown': 'Unknown', 'NULL': 'Unknown'})
    chunk['ASACLAS'] = chunk['ASACLAS'].map({'None assigned': 'Unknown', 'NULL': 'Unknown'})
    chunk['SSSI'] = (chunk['SUPINFEC'] == 'Superficial Incisional SSI') & (chunk['SSSIPATOS'] != 'Yes')
    chunk['DSSI'] = (chunk['WNDINFD'] == 'Deep Incisional SSI') & (chunk['DSSIPATOS'] != 'Yes')
    chunk['OSSI'] = (chunk['ORGSPCSSI'] == 'Organ/Space SSI') & (chunk['OSSIPATOS'] != 'Yes')
    chunk['WOUND'] = chunk['DEHIS'] == 'Wound Disruption'
    chunk['PNEUMONIA'] = (chunk['OUPNEUMO'] == 'Pneumonia') & (chunk['PNAPATOS'] != 'Yes')
    chunk['UNPLANNED_INT'] = chunk['REINTUB'] == 'Unplanned Intubation'
    chunk['PE'] = chunk['PULEMBOL'] == 'Pulmonary Embolism'
    chunk['VENT48'] = (chunk['FAILWEAN'] == 'On Ventilator greater than 48 Hours') & (chunk['VENTPATOS'] != 'Yes')
    chunk['PRI'] = chunk['RENAINSF'] == 'Progressive Renal Insufficiency'
    chunk['ARF'] = chunk['OPRENAFL'] == 'Acute Renal Failure'
    chunk['UTI'] = (chunk['URNINFEC'] == 'Urinary Tract Infection') & (chunk['UTIPATOS'] != 'Yes')
    chunk['STROKE'] = chunk['CNSCVA'] == 'Stroke/CVA'
    chunk['CARD_ARREST_CPR'] = chunk['CDARREST'] == 'Cardiac Arrest Requiring CPR'
    chunk['MI'] = chunk['CDMI'] == 'Myocardial Infarction'
    chunk['DVT1'] = chunk['OTHDVT'] == 'DVT Requiring Therapy'
    chunk['DVT2'] = chunk['OTHDVT'] == 'DVT Requiring Therap'
    chunk['SEPSIS'] = (chunk['OTHSYSEP'] == 'Sepsis') & (chunk['SEPSISPATOS'] != 'Yes')
    chunk['SEPSHOCK'] = (chunk['OTHSESHOCK'] == 'Septic Shock') & (chunk['SEPSHOCKPATOS'] != 'Yes')
    chunk['TRANSFUSION'] = chunk['OTHBLEED'] == 'Transfusions/Intraop/Postop'
    chunk['RETURNOR'] = chunk['RETURNOR'] == 'Yes'
    chunk['DECEASED'] = chunk['DOpertoD'] > -99
    chunk['STILLINHOSP'] = chunk['STILLINHOSP'] == 'Yes'
    chunk['CDIFF'] = chunk['OTHCDIFF'] == 'C. diff'
    chunk['EOL_WDCARE'] = chunk['EOL_WDCARE'] == 'Yes'
    chunk['PREOP_COVID'] = chunk['PREOP_COVID'].map({'Yes, lab-confirmed diagnosis (or ICD-10 code U07.1)': True, 'Yes, suspected diagnosis (or ICD-10 code U07.2)': True, 'No': False, 'NULL': 'Unknown'})
    chunk['POSTOP_COVID'] = chunk['POSTOP_COVID'] == 'Yes, lab-confirmed diagnosis (or ICD-10 code U07.1)'
    chunk['OXYGEN_SUPPORT'] = chunk['OXYGEN_SUPPORT'] == 'Yes'
    chunk['HXFALL'] = chunk['HXFALL'] == 'Yes, within 6 months'
    chunk['HXDEMENTIA'] = chunk['HXDEMENTIA'] == 'Yes'
    chunk['POSTOP_DELIRIUM'] = chunk['DELIRIUM'] == 'Delirium present on screening'
    chunk['DC_SERVICES'] = chunk['DISHOMESVC'] == 'Discharged to home with services'
    chunk['ROBOT_USED'] = chunk['ROBOT_USED'] == 'Yes'
    chunk['HIP_PREOP_DEMENTIA'] = chunk['HIP_PREOP_DEMENTIA'] == 'Yes'
    chunk['HIP_PREOP_DELIRIUM'] = chunk['HIP_PREOP_DELIRIUM'] == 'Yes'
    chunk['HIP_PREOP_BONEMEDS'] = chunk['HIP_PREOP_BONEMEDS'] == 'Yes'
    chunk['HIP_PREOP_MOBAID'] = chunk['HIP_PREOP_MOBAID'] == 'Yes'
    chunk['HIP_PREOP_PRESSORE'] = chunk['HIP_PREOP_PRESSORE'] == 'Yes'
    chunk['HIP_STDCARE'] = chunk['HIP_STDCARE'] == 'Yes'
    chunk['HIP_WBAT_POD1'] = chunk['HIP_WBAT_POD1'] == 'Yes'
    chunk['HIP_DVT_28D'] = chunk['HIP_DVT_28D'] == 'Yes'
    chunk['HIP_POST_PRESSORE'] = chunk['HIP_POST_PRESSORE'] == 'Yes'
    chunk['HIP_POST_DELIRIUM'] = chunk['HIP_POST_DELIRIUM'] == 'Yes'
    chunk['HIP_POST_MOBAID'] = chunk['HIP_POST_MOBAID'] == 'Yes'
    chunk['HIP_POST_BONEMEDS'] = chunk['HIP_POST_BONEMEDS'] == 'Yes'
    chunk = chunk.applymap(lambda x: True if x == 'Yes' else (False if x == 'No' else x))

    # Join columns with the same variables specified under different names between years
    chunk['REOPERATION1'] = chunk['REOPERATION1'].fillna(chunk['REOPERATION'])
    chunk['UNPLANNEDREADMISSION1'] = chunk['UNPLANNEDREADMISSION1'].fillna(chunk['UNPLANREADMISSION'])
    chunk['MORTPROB'] = chunk['MORTPROB'].fillna(chunk['MortProb'])
    chunk['MORBPROB'] = chunk['MORBPROB'].fillna(chunk['MorbProb'])

    # Vectorized operations on the newly joined columns with boolean values
    chunk['REOPERATION1'] = chunk['REOPERATION1'].map({'Yes': True, 'No': False, 'NULL': 'Unknown'})
    chunk['UNPLANNEDREADMISSION1'] = chunk['UNPLANNEDREADMISSION1'].map({'Yes': True, 'No': False, 'NULL': 'Unknown'})

    # Convert ordinal variables
    for column, categories in ordinal_mappings.items():
        if column in chunk.columns:  # Ensure column exists in the current chunk
            chunk[column] = chunk[column].astype('category')
            chunk[column] = chunk[column].cat.set_categories(categories, ordered=True)

    # Employ a bitwise OR operation between DVT1 and DVT2 to merge the two columns and finish accounting for typos
    chunk['DVT'] = chunk['DVT1'] | chunk['DVT2']

    # Now with boolean values assigned to columns, update the types_dict
    boolean_variables = ['HISPANIC', 'SSSI', 'DSSI', 'OSSI', 'WOUND', 'PNEUMONIA', 'UNPINT', 'PE', 'VENT48',
                         'PRI', 'ARF', 'UTI', 'STROKE', 'CAR', 'MI', 'DVT', 'SEPSIS',
                         'SEPSHOCK', 'BLEED', 'DECEASED']

    # Reinforce boolean variable categorization at the end of operations
    for i in boolean_variables:
        types_dict[i] = 'boolean'

    # Calculate BMI and categorize it
    # Convert HEIGHT (in inches) to meters (1 inch = 0.0254 meters)
    # Convert WEIGHT (in pounds) to kilograms (1 pound = 0.453592 kg)
    chunk['HEIGHT_M'] = chunk['HEIGHT'] * 0.0254
    chunk['WEIGHT_KG'] = chunk['WEIGHT'] * 0.453592

    # BMI calculation: BMI = weight(kg) / height(m)^2
    chunk['BMI'] = chunk['WEIGHT_KG'] / (chunk['HEIGHT_M'] ** 2)

    # Create BMI category column based on BMI ranges
    conditions = [
        (chunk['BMI'] < 18.5),
        (chunk['BMI'] >= 18.5) & (chunk['BMI'] < 25),
        (chunk['BMI'] >= 25) & (chunk['BMI'] < 30),
        (chunk['BMI'] >= 30) & (chunk['BMI'] < 35),
        (chunk['BMI'] >= 35) & (chunk['BMI'] < 40),
        (chunk['BMI'] >= 40)
    ]
    bmi_categories = [
        'Underweight: BMI < 18.5',
        'Normal weight: BMI = 18.5 - 24.9',
        'Overweight: BMI = 25 - 29.9',
        'Class 1 Obesity: BMI = 30 - 34.9',
        'Class 2 Obesity: BMI = 35 - 39.9',
        'Class 3 Obesity: BMI > 40'
    ]
    chunk['BMI_Cat'] = np.select(conditions, bmi_categories, default='Unknown')

    # Replace all missing values per chunk (we will still utilize 'NULL' as it is reported in the data sheet)
    missing_values = [-99, 'Unknown', 'None assigned', 'Unknown/Not Reported']
    chunk.replace(missing_values, np.nan, inplace=True)

    # Drop the specified columns
    chunk.drop(columns=columns_to_drop, axis=1, inplace=True)

    # Reset the index before writing
    chunk.reset_index(inplace=True)

    return chunk

# Read and process CSV file in chunks
chunk_size = 500000  # Adjust based on memory capacity
output_file_name = 'nsqip_clean.parquet'  # Include '.parquet' extension for the output file
output_path = Path(r'/Users/Ben/Desktop/Research Stuff/Databases/Combined Files/NSQIP PUF 2008-23')
output_file_path = output_path / output_file_name

first_write = True  # To handle file creation

# List of all columns, excluding those that you plan to drop
filtered_columns = [col for col in all_variables if col not in columns_to_drop]

# Load chunks with relaxed type constraints to avoid error messages
for chunk in pd.read_csv(nsqip, encoding='latin-1', chunksize=chunk_size):
    # Pre-process invalid data in numeric columns using `pd.to_numeric()`
    for col in types_dict:
        if types_dict[col] in [float, int]:  # Only process numeric columns
            if col in chunk.columns:
                # Convert column to numeric, coercing invalid values to NaN
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    # Function to replace empty strings with NaN across all columns
    chunk = chunk.applymap(lambda x: pd.NA if x == "" else x)

    # Explicitly clean boolean-like columns
    boolean_columns = [
        "STILLINHOSP", "REOPERATION2", "REOPERATION3", "READMRELATED1",
        "UNPLANNEDREADMISSION2", "READMRELATED2", "READMRELATED3",
        "READMISSION3", "EOL_WDCARE", "PREOP_COVID"
    ]
    for col in boolean_columns:
        if col in chunk.columns:
            # Coerce to numeric, convert invalid values to NaN, then cast to boolean
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').notna()

        # Fix type inconsistencies in CPT column before writing to Parquet
        if 'CPT' in chunk.columns:
            chunk['CPT'] = chunk['CPT'].astype(str)

        # Fix type inconsistencies in columns that need to be categorized as strings
        string_columns = [
            "OTHERCPT1", "OTHERCPT2", "OTHERCPT3", "OTHERCPT4", "OTHERCPT5", "OTHERCPT6", "OTHERCPT7",
            "OTHERCPT8", "OTHERCPT9", "OTHERCPT10", "CONCPT1", "CONCPT2", "CONCPT3", "CONCPT4",
            "CONCPT5", "CONCPT6", "CONCPT7", "CONCPT8", "CONCPT9", "CONCPT10", "ANESTHES_OTHER",
            "ANESTHES", "PODIAG", "PODIAG10", "RETORRELATED", "REOPOR1ICD101", "RETOR2RELATED",
            "REOPOR2ICD101", "READMSUSPREASON1", "READMRELICD101", "READMRELICD102", "READMSUSPREASON2",
            'REOPORCPT1', 'REOPOR1ICD101', 'REOPOR2CPT1', 'REOPOR2ICD101', 'READMRELICD101',
            'READMRELICD102'
        ]
        for col in string_columns:
            if col in chunk.columns:
                chunk[col] = chunk[col].fillna("NA").astype(str)

        # Fix type inconsistencies in numeric columns
        numeric_columns = ['MORTPROB', 'MORBPROB']
        for col in numeric_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

        # Enforce uniform data types for all columns
        for col in chunk.columns:
            if chunk[col].dtype == object:
                # Ensure object type columns are strings
                chunk[col] = chunk[col].fillna("").astype(str)
            elif chunk[col].dtype.name in ["float64", "int64"]:
                # Ensure numeric types are consistent
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

        # Perform a final type validation step (strict check for boolean consistency)
        for col in chunk.columns:
            if pd.api.types.is_bool_dtype(chunk[col]):
                # Ensure boolean columns contain only True, False, or NaN
                chunk[col] = chunk[col].map(lambda x: x if pd.notna(x) else False)

        # Remove duplicate columns from both the DataFrame and filtered_columns
        filtered_columns = pd.Index(filtered_columns).drop_duplicates().tolist()
        chunk = chunk.loc[:, ~chunk.columns.duplicated()]

        # Filter filtered_columns to include only columns present in the chunk
        valid_columns = [col for col in filtered_columns if col in chunk.columns]

        # Handle columns that are missing from the chunk
        if not valid_columns:
            print("Warning: No valid columns found in this chunk. Skipping.")
            continue  # Skip current chunk if none of the required columns are present

        # Ensure chunk contains only valid_columns only
        chunk = chunk[valid_columns]

        # Write each processed chunk to Parquet
        chunk.to_feather(output_file_path, index=False, append=not first_write)
        first_write = False  # Ensure subsequent chunks are appended

print(f"Processing complete. Output saved to {output_file_path}")
