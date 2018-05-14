import utils
from os.path import isfile, join
from os import listdir
import re

def populate_concept_level_performance(complete_validation_file, c_map_file):
    if isfile(c_map_file):
        return utils.load_json_data(c_map_file)
    lines = utils.read_text_file(complete_validation_file)
    concept2label = {}
    for l in lines[1:]:
        arr = l.split('\t')
        label = arr[2]
        concept = arr[8]
        c_map = None
        if concept not in concept2label:
            c_map = {}
            concept2label[concept] = c_map
        else:
            c_map = concept2label[concept]
        if label not in c_map:
            c_map[label] = 1
        else:
            c_map[label] += 1
    utils.save_json_array(concept2label, c_map_file)
    return concept2label


def populate_phenotype_validation_results(phenotype_def_file,
                                          complete_validation_file, c_map_file,
                                          output_file):
    c_map = populate_concept_level_performance(complete_validation_file, c_map_file)
    phenotypes = utils.load_json_data(phenotype_def_file)
    for p_name in phenotypes:
        p = phenotypes[p_name]
        p['validation'] = {}
        for c in p['concepts']:
            if c not in c_map:
                continue
            for label in c_map[c]:
                if label in p['validation']:
                    p['validation'][label] += c_map[c][label]
                else:
                    p['validation'][label] = c_map[c][label]
    utils.save_json_array(phenotypes, output_file)
    print 'done'


def do_phenotype_result():
    populate_phenotype_validation_results('./data/phenotype_def_file',
                                          './data/compelete_validat_file',
                                          './data/c_map_file.json', './data/phenotype_def_with_validation.json')


def do_phenotype_analysis(phenotype_result_file, c_map_file, output_folder):
    c_map = utils.load_json_data(c_map_file)
    p_map = utils.load_json_data(phenotype_result_file)
    # extract performances of phenotypes
    headers = ["posM", "hisM", "negM", "otherM", "wrongM"]
    rows = ['\t'.join(["phenotype"] + headers)]
    for p in p_map:
        v = p_map[p]['validation']
        if v is None or len(v) == 0:
            continue
        rows.append('\t'.join([p] + [str(v[h]) if h in v else '0' for h in headers]))
    utils.save_string('\n'.join(rows), join(output_folder, 'phenotype_performance.tsv'))


def add_concept_level_freqs(data_folder, c_map_file):
    reg_p = re.compile(".*annotations\\.csv")
    c_map = utils.load_json_data(c_map_file)
    for f in listdir(data_folder):
        if reg_p is not None:
            m = reg_p.match(f)
            if m is not None:
                print '%s matched, reading...' % f
                lines = utils.read_text_file(join(data_folder, f))
                for l in lines:
                    arr = l.split('\t')
                    if arr[0] not in c_map:
                        continue
                    if 'freq' not in c_map[arr[0]]:
                        c_map[arr[0]]['freq'] = 0
                    c_map[arr[0]]['freq'] += int(arr[1])
    utils.save_json_array(c_map, c_map_file)


def output_phenotypes(phenotype_file, phenotype_performance, c_map_file, output_file):
    p = utils.load_json_data(phenotype_file)
    c_map = utils.load_json_data(c_map_file)
    new_p = {}
    p_lines = utils.read_text_file(phenotype_performance)
    for l in p_lines[1:]:
        arr = l.split('\t')
        new_p[arr[0]] = p[arr[0]]
        pt = new_p[arr[0]]
        concepts = pt['concepts']
        pt['concepts'] = {}
        pt['prevalence'] = 0
        for c in concepts:
            pt['concepts'][c] = 0 if c not in c_map else c_map[c]['freq']
            pt['prevalence'] += pt['concepts'][c]
    utils.save_json_array(new_p, output_file)
    print 'new data saved to %s' % output_file


def phenotype_prevalence(phenotype_with_prev, output_file):
    pd = utils.load_json_data(phenotype_with_prev)
    utils.save_string('\n'.join(['\t'.join([p, str(pd[p]['prevalence']), str(len(pd[p]['concepts']))]) for p in pd]),
                      output_file)


def output_single_phenotype_detail(pprevalence_file, phenotype, output_file):
    pp = utils.load_json_data(pprevalence_file)
    p = pp[phenotype]
    rows = []
    rows.append('\t'.join(['total', str(p['prevalence'])]))
    for sp in p['subtypes']:
        rows.append('\t'.join([sp['phenotype'], str(p['concepts'][sp['concept']])]))
    for c in p['concepts']:
        rows.append('\t'.join([c, str(p['concepts'][c])]))
    utils.save_string('\n'.join(rows), output_file)
    print '% result saved to %s' % (phenotype, output_file)


def patient_level_analysis(complete_anns_file, output_file):
    lines = utils.read_text_file(complete_anns_file)
    pos_condition2patients = {}
    patient2conditions = {}
    positive_labels = ['posM', 'hisM']
    indexable_labels = ['posM', 'hisM', 'negM']
    for l in lines:
        arr = l.split('\t')
        label = arr[2]
        condition = arr[3]
        pid = arr[8]
        if label in positive_labels:
            pos_condition2patients[condition] = [pid] if condition not in pos_condition2patients else \
                pos_condition2patients[condition] + [pid]
        if label in indexable_labels:
            pd = patient2conditions[pid] if pid in patient2conditions else {}
            patient2conditions[pid] = pd
            if label in pd:
                pd[label].append(condition)
                pd[label] = list(set(pd[label]))
            else:
                pd[label] = [condition]
    utils.save_json_array({'p2c': patient2conditions, 'c2p': pos_condition2patients}, output_file)


if __name__ == "__main__":
    # do_phenotype_analysis('./data/phenotype_def_with_validation.json', './data/c_map_file.json', './data/pstats/')
    # add_concept_level_freqs('./data/', './data/c_map_file.json')
    # output_phenotypes('./data/phenotype_def_with_validation.json',
    #                   './data/pstats/phenotype_performance.tsv',
    #                   './data/c_map_file.json',
    #                   './data/phenotype_with_prevlence.json')
    # phenotype_prevalence('./data/phenotype_with_prevlence.json', './data/pprevalence.tsv')
    # output_single_phenotype_detail('./data/phenotype_with_prevlence.json', 'Cerebrovascular Disease', './data/Cerebrovascular_Disease.tsv')
    patient_level_analysis('/Users/honghan.wu/Documents/UoE/working_papers/text2phenome/completed_anns.tsv',
                           '/Users/honghan.wu/Documents/UoE/working_papers/text2phenome/condition_patient_dicts.json')
