from semquery import SemEHRES
from datetime import timedelta, datetime
import utils
from os.path import isfile, join
from os import listdir
import json


def time_window_filtering(p2time, d2time, ann, patient):
    d_time = None if ann['appearances'][0]['eprid'] not in d2time else d2time[ann['appearances'][0]['eprid']]
    p_time = None if patient['_id'] not in p2time else p2time[patient['_id']]
    if d_time is None or p_time is None:
        return True # TODO: discussion needed here, the default action when no time info is available
    else:
        return timedelta(days=-365) + p_time <= d_time <= timedelta(days=7) + p_time


def load_patient_date(patient_date_file):
    lines = utils.read_text_file(patient_date_file)
    print 'patient file read. parsing...'
    p2time = {}
    for l in lines[1:]:
        arr = l.split(',')
        p2time[arr[0]] = datetime.strptime(arr[1], '%d/%m/%Y')
    return p2time


def load_doc_date(doc_date_file):
    lines = utils.read_text_file(doc_date_file)
    print 'doc file read. parsing...'
    d2time = {}
    for l in lines[1:]:
        arr = l.split('\t')
        d2time[arr[0]] = datetime.strptime(arr[2], '%Y-%m-%d %H:%M:%S.%f')
    return d2time


def query_hepc_results():
    # load patient and document dates
    p2time = load_patient_date('U:/kconnect/complete_addiction_cohort_entry_date.csv')
    d2time = load_doc_date('U:/kconnect/doc_dates.txt')
    es = SemEHRES.get_instance()
    results, docs = es.summary_patients_by_concepts(['C0019196', 'C2148557', 'C0220847'], time_window_filtering,
                                              args=[p2time, d2time])
    utils.save_json_array(results, './addiction_res/hepc_results.json')
    utils.save_json_array(docs, './addiction_res/hepc_valid_docs.json')


def query_drugs_results(drug, concepts):
    # load patient and document dates
    es = SemEHRES.get_instance()
    results, _ = es.summary_patients_by_concepts(concepts)
    utils.save_json_array(results, './addiction_res/%s_results.json' % drug)


def merge_and_output(dir_path):
    headers = ['all', 'positive', 'Negated', 'hypothetical', 'historical', 'Other']

    results = {}
    c_results = utils.load_json_data(join(dir_path, 'hepc_results.json'))
    for p in c_results:
        results[p['id']] = p
    for f in [f for f in listdir(dir_path) if isfile(join(dir_path, f))]:
        c_results = utils.load_json_data(join(dir_path, f))
        if f != 'hepc_results.json':
            d = f.replace('_results.json', '')
            if d not in headers:
                headers.append(d)
            for p in c_results:
                results[p['id']][d] = p['all']

    s = '\t'.join(['id'] + headers) + '\n'
    for pid in results:
        p = results[pid]
        row = [pid] + ['-' if h not in p else str(p[h]) for h in headers]
        s += '\t'.join(row) + '\n'
    print s


def print_manual_checked_liver_concepts():
    concepts = utils.load_json_data('./concept_maps/liver_disease_concepts.json')
    checked = utils.load_json_data('./concept_maps/liver_diseases_checked.json')
    chked_concepts = []
    for cname in checked:
        if checked[cname] == 'correct':
            chked_concepts.append(concepts[cname]['mapped'])
    print json.dumps(chked_concepts)

if __name__ == "__main__":
    # query_hepc_results()
    # query_drugs_results('RIBAVIRIN', ['C1382829', 'C1128545', 'C0035525'])
    # query_drugs_results('PEGINTERFERON ALPHA',
    #                     ['C0982327','C0907160','C0279030','C0021747','C2599808','C0021734','C0002199','C3165060'])
    # query_drugs_results('SOFOSBUVIR', ['C2976303'])
    # query_drugs_results('DACLATASVIR', ['C3252090'])
    # query_drugs_results('LEDIPASVIR', ['C3851350'])
    # query_drugs_results('RITONAVIR', ['C0992889','C0939237','C0292818','C2741195','C1676707'])
    # query_drugs_results('BOCEPREVIR', ['C1738934'])
    # query_drugs_results('TELAPREVIR', ['c1876229'])
    # print(timedelta(days=-365) + datetime.strptime('2016-02-08 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f'))
    # merge_and_output('./addiction_res/')
    print_manual_checked_liver_concepts()