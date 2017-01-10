
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

# Using column index number for source to deal with inconsistent headers
mapping = {
    'Observation':0,
    'Data_Marking':1,
    'ValueDomain':4,
    'Obs_Type':6,
    'Obs_Type_Value':8,
    'Unit_of_Measure_Eng':10,
    'Geographic_Area':14,
    'Time':17,
    'Time_Type':20,
    'CDID':25,
}


# Load source file and create new blank dataframe

import pandas as pd
import sys

load_file = sys.argv[1]
source = pd.read_csv(load_file, dtype=object)

newDF = pd.DataFrame(columns=standardDims)


# mapping for standard columns
for key in mapping.keys():
    newDF[key] = source.ix[:,mapping[key]]


# how many 'topic' dimensions do we have
numberTopics = int((len(source.columns.values) - 35) / 8)


# For each topic dimensions output our name and value
for T in range(1, numberTopics+1):
    num = str(T).replace('.0', '') # in case of pandas and its decimal-ing strings thing /sigh
    newDF['Dimension_' + num] = source['dim_id_' + num]
    newDF['Dimension_Value_' + num] = source['dim_item_id_' + num]

# Clean out any Nan and Null values
newDF.fillna('', inplace = True)

# Output
newDF.to_csv('ReMapped-' + load_file, index=False)

