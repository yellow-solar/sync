""" Function to create config variables to use"""

# Import json library
import json


def config(filename='config.json', section='yellowpgdbdev'):
    """ function to return config of input and section """
   # Import from config file
    with open(filename) as json_data_file:
        cfg = json.load(json_data_file)
    # process section
    return_cfg = cfg
    if (section in cfg):
        return_cfg = cfg[section]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return (return_cfg)

