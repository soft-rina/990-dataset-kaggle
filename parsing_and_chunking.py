import pandas as pd
import csv
import xmltodict
import xml
import os 
from pprint import pprint
from itertools import chain, islice
#from IPython.display import display

# Batching function
def batched(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Function to flatten nested dictionary
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

# Function to process data in chunks
def chunking_csv(l):
    for i, batch in enumerate(batched(l, 5000)):
        process_chunk(batch, '{}_chunk.csv'.format(i))
        #print(len(batch))
        

def process_chunk(batch, dst):
    # Create empty list to store dictionaries
    dict_list = []

    for file in batch:
        with open(file) as fd:
            try:
                doc = flatten(xmltodict.parse(fd.read()), '', {})
            except xml.parsers.expat.ExpatError as exc:
                print("Failed to parse file {} with error: {}".format(file, str(exc)))
        dict_list.append(doc)

    # Finding list of unique keys 
    uniqueKeyList = list(set(chain.from_iterable(value.keys() for value in dict_list)))

    # Creating dataframe from list of dict
    df = pd.DataFrame(dict_list, 
                    columns=uniqueKeyList)
    
    # Exporting into csv file
    df.to_csv(dst, encoding='utf-8', index=False)


# Create list of element names in directory
#directory = os.path.expanduser('~/Downloads/Form_990_Series_(e-file)_XML_2020/')
directory = os.path.expanduser('~/Downloads/Form_990_Series_(e-file)_XML_2020/download990xml_2020_1')
elems = []
for root, dirs, files in os.walk(directory):
    for filename in files:
            if filename.endswith('.xml'):
                elems.append(os.path.join(root, filename))

print(len(elems))

chunking_csv(elems)
