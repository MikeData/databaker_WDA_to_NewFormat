
# coding: utf-8



# constants

standardDims = [
'Observation',
'Data_Marking',
'ValueDomain',
'Obs_Type',
'Obs_Type_Value',
'Unit_of_Measure_Eng',
'Geographic_Area',
'Time',
'Time_Type',
'CDID',
]

topicDims = [
'Dimension_',
'Dimension_Value_'
]

mapping = {
    'Observation':'observation',
    'Data_Marking':'data_marking',
    'ValueDomain':'measure_type_eng',
    'Obs_Type':'observation_type',
    'Obs_Type_Value':'obs_type_value',
    'Unit_of_Measure_Eng':'unit_of_measure_eng',
    'Geographic_Area':'geographic_area',
    'Time':'time_dim_item_id',
    'Time_Type':'time_type',
    'CDID':'cdid',
}


# Load source file and create new blank dataframe

import pandas as pd
import sys

load_file = sys.argv[1]
source = pd.read_csv(load_file, dtype=object)

newDF = pd.DataFrame(columns=standardDims)


# gonna lower-case the source column names, in case of SAS source (they like caps)
cols = source.columns.values
cols = [x.lower() for x in cols]
source.columns = cols


# mapping for standard columns
for key in mapping.keys():
    newDF[key] = source[mapping[key]]


# how many 'topic' dimensions do we have
numberTopics = int((len(source.columns.values) - 32) / 8)


# For each topic dimensions output our name and value
for T in range(1, numberTopics+1):
    num = str(T).replace('.0', '') # in case of pandas and its decimal-ing strings thing /sigh
    newDF['Dimension_' + num] = source['dim_id_' + num]
    newDF['Dimension_Value_' + num] = source['dim_item_id_' + num]

# Clean out any Nan and Null values
newDF.fillna('', inplace = True)

# Output
newDF.to_csv('ReMapped-' + load_file, index=False)

