import pandas as pd
import csv
import xmltodict
import xml
import os 
from pprint import pprint
from itertools import chain
#from IPython.display import display

# Create list of element names in directory
#directory = os.path.expanduser('~/Downloads/Form_990_Series_(e-file)_XML_2020/')
directory = os.path.expanduser('~/Downloads/Form_990_Series_(e-file)_XML_2020/download990xml_2020_1')
elems = []
for root, dirs, files in os.walk(directory):
    for filename in files:
            if filename.endswith('.xml'):
                elems.append(os.path.join(root, filename))

print(len(elems))

#Function to flatten nested dictionary
def flatten(current, key, result):
    if isinstance(current, dict):
        for k in current:
            new_key = "{0}.{1}".format(key, k) if len(key) > 0 else k
            flatten(current[k], new_key, result)
    elif isinstance(current, list):
        for i,v in enumerate(current):
            new_key = "{0}.{1}".format(key, i) if len(key) > 0 else str(i)
            flatten(v, new_key, result)
    else:
        result[key] = current
    return result

# Create empty list to store dictionaries
dict_list = []

#Reading xml file
for file in elems:
    with open(file) as fd:
        try:
            doc = flatten(xmltodict.parse(fd.read()), '', {})
        except xml.parsers.expat.ExpatError as exc:
            print("Failed to parse file {} with error: {}".format(file, str(exc)))

    dict_list.append(doc)
    # comment when done
    #break

print("Num files:",len(dict_list))


# Finding list of unique keys 
uniqueKeyList = list(set(chain.from_iterable(value.keys() for value in dict_list)))

# Printing the list
#print(uniqueKeyList)

# Creating dataframe from list of dict
df = pd.DataFrame(dict_list, 
                   columns=uniqueKeyList)

#print(df)

# Exporting into csv file
df.to_csv('first_folder.csv', encoding='utf-8', index=False)
