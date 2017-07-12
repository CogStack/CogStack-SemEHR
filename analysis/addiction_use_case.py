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


def first_time_collector(d2time, ann, patient_obj, ann_type):
    if ann_type != 'positive':
        return
    prp_key = 'first_pos_time'
    d_time = None if ann['appearances'][0]['eprid'] not in d2time else d2time[ann['appearances'][0]['eprid']]
    print d_time
    if d_time is not None and patient_obj is not None:
        patient_obj[prp_key] = d_time if prp_key not in patient_obj else \
            (patient_obj[prp_key] if d_time > patient_obj[prp_key] else d_time)
        print '%s is %s' %(patient_obj, patient_obj[prp_key])


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


def read_patient_time_windows(time_file_loc):
    # load patient and document dates
    p2time = load_patient_date('%s/complete_addiction_cohort_entry_date.csv' % time_file_loc)
    patient_filter = [p for p in p2time]
    d2time = load_doc_date('%s/doc_dates.txt' % time_file_loc)
    return patient_filter, p2time, d2time


def query_hepc_results(concepts, prefix, patient_filter, p2time, d2time):
    es = SemEHRES.get_instance()
    results, docs = es.summary_patients_by_concepts(concepts, time_window_filtering,
                                                    args=[p2time, d2time], patient_filters=patient_filter)
    utils.save_json_array(results, './addiction_res/%s_results.json' % prefix)
    utils.save_json_array(docs, './valid_doc_files/%s_valid_docs.json' % prefix)


def query_liver_diseases(concepts, prefix, patient_filter, d2time):
    es = SemEHRES.get_instance()
    results, docs = es.summary_patients_by_concepts(concepts,
                                                    filter_func=None,
                                                    args=[d2time],
                                                    patient_filters=patient_filter,
                                                    data_collection_func=first_time_collector)
    utils.save_json_array(results, './addiction_res/%s_results.json' % prefix)
    utils.save_json_array(docs, './valid_doc_files/%s_valid_docs.json' % prefix)


def query_drugs_results(drug, concepts):
    # load patient and document dates
    es = SemEHRES.get_instance()
    results, _ = es.summary_patients_by_concepts(concepts)
    utils.save_json_array(results, './addiction_res/%s_results.json' % drug)


def merge_and_output(dir_path, cohort, default_results='hepc_results.json'):
    headers = ['all', 'positive', 'Negated', 'hypothetical', 'historical', 'Other', 'first_pos_time']

    results = {}
    for pid in cohort:
        results[pid] = {}
    c_results = utils.load_json_data(join(dir_path, default_results))
    for p in c_results:
        results[p['id']] = p
    for f in [f for f in listdir(dir_path) if isfile(join(dir_path, f))]:
        if f != default_results:
            c_results = utils.load_json_data(join(dir_path, f))
            d = f.replace('_results.json', '')
            print f
            if d not in headers:
                headers.append(d)
            for p in c_results:
                results[p['id']][d] = p['all']

    s = '\t'.join(['id'] + headers) + '\n'
    for pid in results:
        p = results[pid]
        row = [pid] + ['-' if h not in p else str(p[h]) for h in headers]
        s += '\t'.join(row) + '\n'
    utils.save_string(s, './valid_doc_files/merged_output_liverdiseases.tsv')
    print 'output generated'

if __name__ == "__main__":
    ps, p2time, d2time = read_patient_time_windows('.')
    pids = utils.read_text_file('./valid_doc_files/hcv_full_cohort.csv')
    ps = pids
    query_hepc_results(["C0679412", "C0019187", "C0400966", "C0020541", "C0015695", "C2718067", "C0015695",
                        "C0151763", "C2711227", "C0023896", "C0023892", "C0152254", "C0023891", "C0001306",
                        "C0238065", "C0085605", "C0524610", "C0015696", "C0341439", "C0348754", "C0023890"]
                       , 'liver_diseases', ps, p2time, d2time)
    # query_hepc_results(['C0019196', 'C2148557', 'C0220847'], 'hepc', ps, p2time, d2time)
    #query_hepc_results(['C1382829', 'C1128545', 'C0035525'], 'RIBAVIRIN', ps, p2time, d2time)
    #query_hepc_results(['C0982327','C0907160','C0279030','C0021747','C2599808','C0021734','C0002199','C3165060'],
    #                   'PEGINTERFERON ALPHA', ps, p2time, d2time)
    #query_hepc_results(['C2976303'], 'SOFOSBUVIR', ps, p2time, d2time)
    #query_hepc_results(['C3252090'], 'DACLATASVIR', ps, p2time, d2time)
    #query_hepc_results(['C3851350'], 'LEDIPASVIR', ps, p2time, d2time)
    #query_hepc_results(['C0992889','C0939237','C0292818','C2741195','C1676707'], 'RITONAVIR', ps, p2time, d2time)
    #query_hepc_results(['C1738934'], 'BOCEPREVIR', ps, p2time, d2time)
    #query_hepc_results(['c1876229'], 'TELAPREVIR', ps, p2time, d2time)
    merge_and_output('./addiction_res/', pids, 'liver_diseases_results.json')
