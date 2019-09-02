import utils
import re
import json
import joblib as jl
from study_analyzer import StudyAnalyzer, StudyConcept, load_ruler, load_study_settings
from os.path import isfile, join
import logging
from concept_mapping import get_concepts_names, get_umls
from concept_mapping import icd10_wildcard_queries, get_umls_client_inst


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


def load_study_ruler(study_folder, rule_config_file, study_config='study.json'):
    sa = None
    if study_folder is not None and study_folder != '':
        r = utils.load_json_data(join(study_folder, study_config))

        ret = load_study_settings(study_folder,
                                  umls_instance=None,
                                  rule_setting_file=r['rule_setting_file'],
                                  concept_filter_file=None if 'concept_filter_file' not in r else r['concept_filter_file'],
                                  do_disjoint_computing=True if 'do_disjoint' not in r else r['do_disjoint'],
                                  export_study_concept_only=False if 'export_study_concept' not in r else r['export_study_concept']
                                  )
        sa = ret['study_analyzer']
        ruler = ret['ruler']
    else:
        logging.info('no study configuration provided, applying rules to all annotations...')
        ruler = load_ruler(rule_config_file)
    return {'sa': sa, 'ruler': ruler}


def break_down_study_concepts(scs, umls, new_mapping_file):
    mmc = {}
    for sc in scs:
        cui = sc.term_to_concept[sc.terms[0]]['mapped']
        m = {
            "tc": {
                "closure": 1,
                "mapped": cui
            },
            "concepts": [cui]
        }
        mmc[sc.name] = m

        c2n = get_concepts_names(umls, list(sc.concept_closure))

        for c in sc.concept_closure:
            if c != cui:
                # for each single concept create a studyconcept
                mc = {
                    "tc": {
                        "closure": 1,
                        "mapped": c
                    },
                    "concepts": [c]
                }
                mmc[c2n[c]] = mc
    utils.save_json_array(mmc, new_mapping_file)


def regenerate_manual_mapped_concepts(tsv, closure_file):
    selected_concepts = set()
    c2l = {}
    for l in utils.read_text_file(tsv):
        arr = l.split('\t')
        selected_concepts.add(arr[1])
        c2l[arr[1]] = arr[0]
    t2closure = utils.load_json_data(closure_file)
    mapped_concepts = []
    map = {}
    v_map = {}
    for t in t2closure:
        disjoint_list = list(set(t2closure[t]) & selected_concepts)
        if len(disjoint_list) > 0:
            mapped_concepts += disjoint_list
            map[t] = {
                "tc":
                    {
                        "closure": len(disjoint_list), "mapped": disjoint_list[0]
                    },

                "concepts": disjoint_list
            }
            v_map[t] = [('%s [%s]' % (c2l[c], c)) for c in disjoint_list]
    print json.dumps(map)
    print selected_concepts - set(mapped_concepts)
    print json.dumps(v_map)


def icd10_mapping_convert(json_file, output_json):
    c2concepts = utils.load_json_data(json_file)
    result = {}
    for c in c2concepts:
        r = {
            "tc": {
                      "closure": len(c2concepts[c]),
                      "mapped": c2concepts[c][0]
                  },
                "concepts": c2concepts[c]
            }
        result[c] = r
    utils.save_json_array(result, output_json)
    logging.info('all done')


def process_icd_to_umls(icds, icd2umls=None):
    lines = []
    concepts = []
    for icd in icds:
        if icd2umls is not None and icd in icd2umls:
            concepts.append(icd2umls[icd])
        else:
            lines.append('%s\ticd' % icd)
    icd2umls = icd10_wildcard_queries(lines)
    for icd in icd2umls:
        concepts += icd2umls[icd]
    return concepts


def parsing_tsv_to_manual_mapped(tsv_file, icd2umls_file):
    icd2umls = {}
    for l in utils.read_text_file(icd2umls_file):
        cols = l.split('\t')
        icd2umls[cols[0]] = cols[1]

    lines = utils.read_text_file(tsv_file)
    condition2code = {}
    for l in lines:
        cols = l.split('\t')
        c = cols[0]
        icds = cols[len(cols) - 1].split(',')
        concepts = []
        condition2code[c] = concepts
        for icd in icds:
            icd = icd.strip().upper()
            icd_codes = []
            m = re.match(r'([A-Z])(\d+)\-[A-Z]{0,1}(\d+)', icd)
            if m is not None:
                logging.info('range mappings: %s' % m.group(0))
                for num in range(int(m.group(2)), int(m.group(3)), 1):
                    icd = '%s%02d' % (m.group(1), num)
                    icd_codes.append(icd)
            else:
                icd_codes.append(icd)
            concepts += process_icd_to_umls(icd_codes, icd2umls=icd2umls)
    logging.info(json.dumps(condition2code))


def extend_manual_mappings(mapping_file, new_mapping_file):
    umls = get_umls_client_inst('./resources/HW_UMLS_KEY.txt')
    m = utils.load_json_data(mapping_file)
    for k in m:
        logging.info('working on %s' % k)
        new_concepts = [] + m[k]['concepts']
        for c in m[k]['concepts']:
            new_concepts += umls.transitive_narrower(c)
        m[k]['concepts'] = list(set(new_concepts))
    logging.info('saving new results to %s' % new_mapping_file)
    utils.save_json_array(m, new_mapping_file)


if __name__ == "__main__":
    logging.basicConfig(level='INFO')
    # generate_hpo_umls_mapping('./resources/hp.obo')
    # export_pickled_study_concept_2_flat_json('', '')
    # s = load_study_ruler('./studies/IMPARTS/', rule_config_file=None)
    # break_down_study_concepts(s['sa'].study_concepts, get_umls(), './studies/IMPARTS/broken_down_mappings.json')
    # regenerate_manual_mapped_concepts('./studies/autoimmune.v3/label2concept.tsv',
    #                                   './studies/autoimmune.v3/sc2closure.json')
    # icd10_mapping_convert('./studies/sickle/icd10_mapping.json', './studies/sickle/manual_mapped_concepts.json')
    # parsing_tsv_to_manual_mapped('./studies/ktr_charlson/charlson_icd10_top_priority.tsv',
    #                              './onto_res/icd10_umls.tsv')
    # icd10_mapping_convert('./studies/ktr_charlson/charlson_icd10_top_priority.json',
    #                       './studies/ktr_charlson/manual_mapped_concepts.json')
    extend_manual_mappings('./studies/ktr_charlson/manual_mapped_concepts.json',
                           './studies/ktr_charlson/manual_mapped_concepts_extended.json')