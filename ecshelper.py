import json, csv, pprint
from collections import OrderedDict
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

rows = []

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('index', help='name of the index')

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def get_mapping(file):
    with open(file, 'r') as mapping:
        mapping = json.load(mapping, object_pairs_hook=OrderedDict)
        return mapping

def unpack(mapping):
    index = list(mapping.keys())[0]
    doc_type = list(mapping[index]['mappings'].keys())[0]
    fields_dict = mapping[index]['mappings'][doc_type]['properties']
    return fields_dict, index

def get_schema(fields_dict):
    schema = {}
    tlf = list(fields_dict.keys())
    for field in tlf:
        if 'type' in fields_dict[field].keys():
            schema[field] = fields_dict[field]['type']
        elif 'properties' in fields_dict[field].keys():
            schema[field] = get_schema(fields_dict[field]['properties'])
    return schema


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                for d in dict_generator(value, [key] + pre):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    for d in dict_generator(v, [key] + pre):
                        yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


'''
with open(f'{index}_schema.csv', 'w') as csvfile:
    fieldnames = ['field', 'type']
    writer = csv.writer(csvfile)
    writer.writerow(fieldnames)
    for k, v in flat_schema.items():
        writer.writerow((k,v))
'''

def get_ecs_fields():
    with open('ecs_fields.csv', 'r') as ff:
        ff_csv = csv.reader(ff)
        header = next(ff_csv)
        fields = [column[0] for column in ff_csv]
        return fields

def get_field_map(ecs_fields, fields):
    field_map = {}
    ecs_completer = WordCompleter(ecs_fields, sentence=True)
    for field in fields:
        field_map[field] = prompt(field + ' > ', completer = ecs_completer)
    return field_map

mapping = get_mapping('bro_mapping.json')
fields_dict, index = unpack(mapping)
schema = get_schema(fields_dict)
flat_schema = flatten_json(schema)
ecs_fields = get_ecs_fields()
field_map = get_field_map(ecs_fields, flat_schema.keys())
print(field_map)
