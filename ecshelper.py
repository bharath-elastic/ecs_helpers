import argparse, json, csv
from pprint import pprint
from collections import OrderedDict
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from config import esconfig

rows = []

def get_credentials():
    user = input('Username: ')
    if user:
        pwd = getpass.getpass()
        return (user, pwd)
    else:
        return(None,None)

def get_clients(user=None,pwd=None):
    host = esconfig[0]['host']
    port = esconfig[0]['port']
    if user:
        es = Elasticsearch([host], http_auth=(user,pwd), port=port)
    else:
        es = Elasticsearch(esconfig)
    ic = IndicesClient(es)
    return es,ic

def export_mapping(ic,index_name):
    mapping = ic.get_mapping(index=args.index)
    with open(f'{index_name}_mapping.json', 'w') as mf:
        json.dump(mapping[index_name], mf)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('index', help='name of the index')
    args = parser.parse_args()
    return args

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
    doc_type = list(mapping['mappings'].keys())[0]
    fields_dict = mapping['mappings'][doc_type]['properties']
    return fields_dict

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

def export_ecsmap(ecsmap):
    with open(f'{index}_ecsmap.json', 'w') as emf:
        json.dump(ecsmap, emf)

def generate_ecs_mappings(ecsmap):
    with open(f'{args.index}_mapping.json', 'r') as omf:
        omd = json.load(omf)

def findnreplace(rmap, mapping):
    mapping_json = json.dumps(mapping)
    for k, v in rmap.items():
        mapping_json = mapping_json.replace(k,v)
    return json.loads(mapping_json)


args = parse_args()
user, pwd = get_credentials()
es, ic = get_clients(user,pwd)
export_mapping(ic,args.index)
mapping = get_mapping(f'{args.index}_mapping.json')
fields_dict = unpack(mapping)
schema = get_schema(fields_dict)
flat_schema = flatten_json(schema)
ecs_fields = get_ecs_fields()
ecsmap = get_field_map(ecs_fields, flat_schema.keys())
#export_ecsmap(ecsmap)
#pprint(ecsmap)
new_mapping = findnreplace(ecsmap, mapping)
#print(new_mapping)
resp = ic.create(index=f'{args.index}_ecs', body=new_mapping)
print(resp)
