import csv
import json
from sys import argv

def convert(from_file, to_file):
    """ Converts the baby-names.csv file to json """
    with open(from_file, 'r') as read_handle, open(to_file, 'w') as write_handle:
        reader = csv.DictReader(read_handle)
        write_handle.writelines(_marshal(entry) for entry in reader)

def _marshal(entry):
    """ Marshalls the csv dict to the correct type """
    data = {
        'year': int(entry['year']),
        'name': entry['name'],
        'percent': float(entry['percent']),
        'sex': entry['sex']
    }
    json_data = json.dumps(data)
    return "{}\n".format(json_data)

if __name__ == '__main__':
    convert(argv[1], argv[2])
