import utils
import re
import json
import joblib as jl
from study_analyzer import StudyAnalyzer, StudyConcept
from os.path import isfile


"""
a script for supporting GeL phenome model, 
e.g., human phenotype ontology mapping generation
"""


def parse_disease_phenotypes(disease_phenotype_csv, disease_model_json):
    lines = utils.read_text_file(disease_phenotype_csv)
    dis_to_data = {}
    for l in lines[1:]:
        arr = l.split(',')
        lv4_id = arr[4]
        lv4_disease = arr[5]
        hpo_label = arr[7]
        hpo_id = arr[8]
        test = arr[9]
        test_id = arr[10]
        dis_data = []
        if lv4_disease in dis_to_data:
            dis_data = dis_to_data[lv4_disease]
        else:
            dis_to_data[lv4_disease] = dis_data
        if len(test.strip()) > 0:
            dis_data.append({'test': test,
                             'test_id': test_id})
        else:
            dis_data.append({'hpo_label': hpo_label,
                             'hpo_id': hpo_id})
    utils.save_json_array(dis_to_data, disease_model_json)


def convert_100k_hpos_to_json():
    csv = '/Users/honghan.wu/git/autoimmune-kconnect/resources/100k/' \
          'Rare Disease Conditions Phenotypes and Clinical Tests  - v1.8.1.csv'
    model_file = '/Users/honghan.wu/git/autoimmune-kconnect/resources/100k/diseae_model.js'
    parse_disease_phenotypes(csv, model_file)


def generate_hpo_umls_mapping(hpo_dump):
    lines = utils.read_text_file(hpo_dump)
    # lines = [u'id: HP:3000076', u'def: "An abnormality', u'xref: UMLS:C4073283']
    maps = []
    cur_map = None
    for l in lines:
        m = re.match(r'^id\: (HP\:\d+)', l)
        if m is not None:
            print 'start with %s' % m.group(1)
            cur_map = {'hp': m.group(1), 'cuis': []}
            maps.append(cur_map)
        m = re.match(r'^xref: (UMLS:C\d+)', l)
        if m is not None:
            cur_map['cuis'].append(m.group(1))
        if l == 'is_obsolete: true':
            cur_map['is_obsolete'] = True
        m = re.match(r'^replaced_by: (HP:\d+)', l)
        if m is not None:
            cur_map['replaced_by'] = m.group(1)

    hpo2umls = {}
    obsolete2replace = {}
    for cur_map in maps:
        hpo2umls[cur_map['hp']] = cur_map['cuis'] if cur_map['hp'] not in hpo2umls \
            else cur_map['cuis'] + hpo2umls[cur_map['hp']]
        if 'is_obsolete' in cur_map and 'replaced_by' in cur_map:
            obsolete2replace[cur_map['hp']] = cur_map['replaced_by']
    for obs in obsolete2replace:
        if obsolete2replace[obs] in hpo2umls:
            hpo2umls[obs] = hpo2umls[obsolete2replace[obs]]
    print json.dumps(hpo2umls)


def export_pickled_study_concept_2_flat_json(pickle_file, output_file):
    if isfile(pickle_file):
        obj = {}
        sa = StudyAnalyzer.deserialise(pickle_file)
        for sc in sa.study_concepts:
            for t in sc.term_to_concepts:
                for c in sc.term_to_concepts[t]['closure']:
                    obj[c] = {"tc": {"closure": 1, "mapped": c}, "concepts": [c]}

        utils.save_json_array(obj, output_file)
        print 'flat json saved to %s' % output_file


if __name__ == "__main__":
    # generate_hpo_umls_mapping('./resources/hp.obo')
    export_pickled_study_concept_2_flat_json('', '')