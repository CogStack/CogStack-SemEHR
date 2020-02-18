import utils
import json
import re
from semquery import SemEHRES
import mimicdao
from random import randint
from os.path import join


def parse_summary_structure(full_text, re_exp=r'^([^\n\:]+)\:$'):
    """
    regular expression based section title extraction
    :param full_text:
    :param re_exp:
    :return:
    """
    matches = re.finditer(re_exp, full_text, re.MULTILINE)
    return [{'section': m.group(1), 'pos': m.span()} for m in matches]


def mapping_FHIR_sections():
    """
    not implemented yet, manual mapping is adopted now
    :return:
    """
    pass


def do_query_analysis(d, container, full_text_field, reg_exp):
    container.append([s['section'] for s in parse_summary_structure(d['_source'][full_text_field], reg_exp=reg_exp)])


def analyse_discharge_summaries(es, q, doc_type='eprdoc',
                                full_text_field='fulltext',
                                reg_exp=r'^([^\n\:]+)\:$',
                                output_file='../resources/wrappers/section_freqs.json'):
    """
    iterate all discharge summaries and create the section dictionary for
    the corpus (EHR system)
    :param es:
    :param q:
    :param doc_type:
    :param full_text_field
    :param reg_exp
    :param output_file
    :return:
    """
    scroll_obj = es.scroll(q, doc_type, include_fields=[full_text_field], size=500)
    container = []
    utils.multi_thread_tasking_it(scroll_obj, 10, do_query_analysis, args=[container, full_text_field, reg_exp])
    print 'search finished. merging sections...'
    sec_freq = {}
    for ss in container:
        for s in ss:
            sec_freq[s] = 1 if s not in sec_freq else 1 + sec_freq[s]
    utils.save_json_array(sec_freq, output_file)
    print json.dumps(sec_freq)
    print 'done'


def normalise_sec_title(s):
    k = re.sub(r'^\d\.\s+.*', '', s.strip())
    k = k.lower()
    return k


def select_section_headers(sec_freq_file):
    """
    do simple syntactic merging of section titles and sort them by
    frequencies
    :param sec_freq_file:
    :return:
    """
    sec_freq = utils.load_json_data(sec_freq_file)
    merged_sec_freq = {}
    for s in sec_freq:
        k = normalise_sec_title(s)
        merged_sec_freq[k] = sec_freq[s] if k not in merged_sec_freq else sec_freq[s] + merged_sec_freq[k]
    sec_freq = merged_sec_freq
    sf = [(s, sec_freq[s]) for s in sec_freq]
    sf = sorted(sf, key=lambda sec: -sec[1])
    utils.save_string('\n'.join('%s\t%s' % t for t in sf), '../resources/wrappers/mimic_section_freqs.txt')


def load_corpus_to_FHIR_mapping(tsv_map_file):
    lines = utils.read_text_file(tsv_map_file)
    sec_to_fhir = {}
    for l in lines:
        arr = l.split('\t')
        for i in range(1, len(arr)):
            sec_to_fhir[arr[i]] = arr[0]
    return sec_to_fhir


def parse_discharge_summary(full_text, anns, corpus_mapping, corpus_re_patther=r'^([^\n\:]+)\:$'):
    # sort anns by offset
    anns = sorted(anns, key=lambda x: x['startNode']['offset'])
    sections = parse_summary_structure(full_text, re_exp=corpus_re_patther)
    checked_ann_idx = 0
    prev_sec = ''
    prev_orig_sec = ''
    prev_pos = 0
    structured_summary = []
    prev_start_offset = 0
    for s in sections:
        normalised_sec = normalise_sec_title(s['section'])
        FHIR_Sec = corpus_mapping[normalised_sec] if normalised_sec in corpus_mapping else None
        if FHIR_Sec is not None:
            checked_ann_idx = put_anns_into_section(full_text, prev_pos, prev_sec, s['pos'][0], anns,
                                                    checked_ann_idx,
                                                    structured_summary, original_sec=prev_orig_sec,
                                                    start_offset=prev_start_offset)
            prev_sec = FHIR_Sec
            prev_orig_sec = s['section']
            prev_pos = s['pos'][0]
            prev_start_offset = s['pos'][1]
    if len(sections) > 0:
        put_anns_into_section(full_text, prev_pos, prev_sec, len(full_text), anns, checked_ann_idx, structured_summary,
                              original_sec=prev_orig_sec)
    # print json.dumps(structured_summary)
    return structured_summary


def put_anns_into_section(full_text, prev_pos, sec, sec_pos, anns, start_index, container,
                          original_sec=None, start_offset=0):
    checked_ann_idx = start_index
    sec_obj = {'section': sec, 'anns': [], 'original_section': original_sec, 'start':prev_pos, 'end': sec_pos}
    sec_obj['text'] = full_text[prev_pos:sec_pos]
    for idx in range(start_index, len(anns)):
        if anns[idx]['startNode']['offset'] < sec_pos:
            if start_offset < anns[idx]['startNode']['offset']:
                sec_obj['anns'].append(anns[idx])
        else:
            checked_ann_idx = idx - 1 if idx > 0 else idx
            break
    container.append(sec_obj)
    return checked_ann_idx


def load_measurement_wrapper():
    return \
        utils.load_json_data('../resources/wrappers/medprofile_extract_wrapper.json'),\
        utils.load_json_data('../resources/wrappers/exp_ms_term_mapping.json')


def extract_patient_measurements(patient_id, es, wrapper, ms_mapping, type2results):
    ds_ids = mimicdao.get_summary_doc_by_patient(patient_id)
    if len(ds_ids) > 0:
        doc = es.get_doc_detail(ds_ids[0]['row_id'])
        corpus_mapping = load_corpus_to_FHIR_mapping('../resources/wrappers/mimic_FHIR_discharge_summary_map.tsv')
        profile = parse_discharge_summary(doc['fulltext'], doc['anns'], corpus_mapping)
        measurements = {}
        for sec in profile:
            if sec['section'] in wrapper['sectsWithMeasures']:
                for ann in sec['anns']:
                    sty = ann['features']['STY']
                    if sty in wrapper['annTypeNeedMeasures']:
                        annEndPos = ann['endNode']['offset'] - sec['start']
                        matches = re.finditer(wrapper['numberPattern'], sec['text'][annEndPos:], re.MULTILINE)
                        for m in matches:
                            if m.span()[0] <= wrapper['maxMeasureDistance']:
                                measurements[ann['features']['PREF']] = m.group(0)
                            break
        correct = []
        incorrect = []
        all = []
        if len(measurements) > 0:
            labs = mimicdao.get_patient_labevents(patient_id)
            for k in ms_mapping:
                if k in measurements:
                    all.append(measurements[k])
                    if ms_mapping[k] in labs:
                        if k not in type2results:
                            type2results[k] = [0, 0]
                        if measurements[k] in labs[ms_mapping[k]]:
                            correct.append((k, measurements[k]))
                            type2results[k][0] += 1
                        else:
                            print patient_id, k, measurements[k]
                            incorrect.append((k, measurements[k]))
                            type2results[k][1] += 1
        print 'correct:%s, incorrect %s, all: %s' % (len(correct), len(incorrect), len(all))
        return len(correct), len(incorrect), len(all)


def sum_complement_data(patient_id, es, wrapper, results):
    ds_ids = mimicdao.get_summary_doc_by_patient(patient_id)
    if len(ds_ids) > 0:
        doc = es.get_doc_detail(ds_ids[0]['row_id'])
        corpus_mapping = load_corpus_to_FHIR_mapping('../resources/wrappers/mimic_FHIR_discharge_summary_map.tsv')
        profile = parse_discharge_summary(doc['fulltext'], doc['anns'], corpus_mapping)
        p_results = {}
        for sec in profile:
            type_to_freq = {}
            if sec['section'] in wrapper['complement_sections']:
                p_results[sec['section']] = {'total': len(sec['anns'])}
                for ann in sec['anns']:
                    type_to_freq[ann['features']['STY']] = 1 if ann['features']['STY'] not in type_to_freq \
                        else type_to_freq[ann['features']['STY']] + 1
                p_results[sec['section']]['t2f'] = type_to_freq
        results.append(p_results)


def random_select_mimic_patients():
    patients = mimicdao.get_all_patient_ids()
    selected = []
    for i in xrange(100):
        rnd_idx = randint(0, len(patients) - 1)
        selected.append(patients[rnd_idx]['subject_id'])
        patients.remove(patients[rnd_idx])
    print selected
    return selected


def extract_measurements(es, random_patients):
    ms_wrapper, ms_mapping = load_measurement_wrapper()
    results = []
    typed_results = {}
    for pid in random_patients:
        results.append(extract_patient_measurements(str(pid), es, ms_wrapper, ms_mapping, typed_results))
    print json.dumps(results)
    for k in typed_results:
        print '%s\t%s\t%s' % (k, typed_results[k][0], typed_results[k][1])


def calculate_complement_data(es, random_patients):
    ms_wrapper, ms_mapping = load_measurement_wrapper()
    results = []
    for pid in random_patients:
        sum_complement_data(pid, es, ms_wrapper, results)

    for s in ms_wrapper['complement_sections']:
        total = 0
        t2freq = {}
        for r in results:
            if s not in r:
                continue
            total += r[s]['total']
            for t in r[s]['t2f']:
                t2freq[t] = r[s]['t2f'][t] if t not in t2freq else t2freq[t] + r[s]['t2f'][t]
        print '%s: %s' % (s, total)
        for t in t2freq:
            print '%s: %s' % (t, t2freq[t])
        print '\n\n'


def mimic_struct_extract_exp(es):
    random_patients = [350, 70100, 10515, 42244, 23473, 14201, 28870, 3348, 84232, 5400, 50148, 98605, 3663, 41710, 80789, 16161, 14839, 16639, 82919, 75741, 88726, 92993, 13800, 25743, 12567, 18797, 78697, 71117, 3136, 54783, 24060, 946, 8552, 30646, 80858, 26966, 23944, 44633, 14678, 15354, 30277, 12596, 2208, 20594, 72847, 84837, 80942, 8473, 122, 11450, 58356, 70684, 138, 80136, 45344, 23289, 41493, 28051, 25349, 62237, 31774, 1746, 45291, 76529, 9884, 23244, 18563, 11889, 40524, 22467, 28331, 31579, 98280, 46321, 28933, 5813, 81597, 12883, 27596, 25625, 500, 28445, 53876, 12527, 11011, 28160, 69763, 23087, 61223, 72760, 14249, 20828, 10022, 24200, 2409, 95495, 54005, 70902, 12880, 23278]
    calculate_complement_data(es, random_patients)


def smp_index(patient_id, es, doc_type):
    """
    structured medical profile indexing
    :param patient_id:
    :param es:
    :param doc_type:
    :return:
    """
    if es.get_doc_detail(patient_id, doc_type):
        print '%s exists in %s' % (patient_id, doc_type)
        return
    print 'indexing %s' % patient_id
    ds_ids = mimicdao.get_summary_doc_by_patient(patient_id)
    if len(ds_ids) > 0:
        doc = es.get_doc_detail(ds_ids[0]['row_id'])
        corpus_mapping = load_corpus_to_FHIR_mapping('../resources/wrappers/mimic_FHIR_discharge_summary_map.tsv')
        profile = parse_discharge_summary(doc['fulltext'], doc['anns'], corpus_mapping)
        mp = {}
        for sec in profile:
            t = sec['section'] if sec['section'] != '' else 'basic'
            t = t.replace(' ', '_')
            mp[t] = sec
        # print json.dumps(mp)
        es.index_med_profile(doc_type, mp, patient_id)
        print '%s indexed' % patient_id


def smp_export(patient_id, es, corpus_mapping, output_folder):
    """
    structured medical profile extraction
    :param es: elasticsearch index
    :param patient_id:
    :param output_folder:
    :return:
    """
    print 'indexing %s' % patient_id
    ds_ids = mimicdao.get_summary_doc_by_patient(patient_id)
    for r in ds_ids:
        doc = es.get_doc_detail(r['row_id'])
        profile = parse_discharge_summary(doc['fulltext'], doc['anns'], corpus_mapping)
        mp = {}
        for sec in profile:
            t = sec['section'] if sec['section'] != '' else 'basic'
            t = t.replace(' ', '_')
            mp[t] = sec
        file_name = '%s_%s.json' % (patient_id, r['row_id'])
        utils.save_json_array(mp, join(output_folder, file_name))
        print '%s indexed' % file_name


def index_mimic_af_cohort_smp():
    med_profile_type = 'medprofile'
    pids = utils.read_text_file('../resources/af_pids.txt')
    print pids
    utils.multi_thread_tasking(pids, 5, smp_index, args=[es, med_profile_type])


def do_collect_pids(d, container):
    container.append(d['_id'])


def query_patients(es, q_obj):
    scroll_obj = es.scroll("", "medprofile", size=300,
                           q_obj=q_obj,
                           include_fields=[])
    container = []
    utils.multi_thread_tasking_it(scroll_obj, 20, do_collect_pids, args=[container])
    return container


def populate_query_using_concepts(field, concepts):
    qo = {"bool": {
          "should": [{ "match": { field: c }} for c in concepts]
        }}
    return qo


def mimic_af_analysis(es):
    pids = utils.read_text_file('../resources/af_pids.txt')
    # print 'querying hypertension...'
    # hyper_phrase_results = query_patients(es, {"match": {"History_of_Past_Illness.text": "hypertension"}})
    # print 'querying af...'
    # af_phrase_results = query_patients(es, {"bool": {
    #       "should": [
    #         { "match_phrase": { "History_of_Past_Illness.text": "atrial fibrillation" }},
    #         { "match_phrase": { "History_of_Past_Illness.text": "paroxysmal atrial fibrillation" }}
    #       ]
    #     }})
    # print 'querying cv...'
    # cv_phrase_results = query_patients(es, {"bool": {
    #       "should": [
    #         { "match_phrase": { "History_of_Past_Illness.text": "Coronary vascular" }},
    #         { "match_phrase": { "History_of_Past_Illness.text": "myocardial infarction" }}
    #       ]
    #     }})
    # print 'querying angina...'
    # angina_phrase_results = query_patients(es, {"match": {"History_of_Past_Illness.text": "angina"}})

    hyper_phrase_results = query_patients(es,
                                          populate_query_using_concepts("History_of_Past_Illness.anns.features.inst",
                                                                        ["C0020538", "C3694763", "C0155607", "C0020545",
                                                                         "C2363973", "C0262395", "C0152171", "C0596515",
                                                                         "C0020539", "C0155620", "C0155621", "C0155622",
                                                                         "C0920394", "C0920393", "C0032914", "C0920747",
                                                                         "C0411176", "C1171328", "C0544619", "C0544618",
                                                                         "C0020542", "C0020540", "C0683382", "C0341934",
                                                                         "C0264936", "C1998407", "C1849552", "C1171349",
                                                                         "C0597854", "C0859775", "C0859765", "C0341950",
                                                                         "C0238780", "C0598428", "C0155598", "C0155596",
                                                                         "C1171326", "C0155594", "C0155595", "C0596088",
                                                                         "C0155591", "C0848548", "C0348860", "C0348586",
                                                                         "C0264650", "C0156664", "C0155606", "C0221155",
                                                                         "C0155604", "C0155605", "C0155593", "C0155609",
                                                                         "C0156669", "C0597048", "C0745133", "C0151620",
                                                                         "C0348587", "C0264641", "C1171351", "C0494574",
                                                                         "C0494575", "C0494576", "C0597853", "C0155584",
                                                                         "C0155587", "C0155586", "C0155589", "C0269658",
                                                                         "C0348879", "C0264655", "C0155617", "C0155616",
                                                                         "C0155611", "C0155610", "C0024588", "C0262534",
                                                                         "C0155619", "C0235222", "C0349368", "C0155583",
                                                                         "C0013537", "C0597290", "C1171363"]))
    af_phrase_results = query_patients(es, populate_query_using_concepts("History_of_Past_Illness.anns.features.inst",
                                                                         ["C0004238", "C2041124", "C0344434",
                                                                          "C0856731", "C0235480", "C1963067",
                                                                          "C3468561", "C0549284"]))
    cv_phrase_results = query_patients(es, populate_query_using_concepts("History_of_Past_Illness.anns.features.inst",
                                                                         ["C0027051", "C0877719", "C1112646",
                                                                          "C1112645", "C0340308", "C0340305",
                                                                          "C0340304", "C1142289", "C0494578",
                                                                          "C1112770", "C2362760", "C0155628",
                                                                          "C0155629", "C0494579", "C0155626",
                                                                          "C0155627", "C0155644", "C0155660",
                                                                          "C0155661", "C0155662", "C0155645",
                                                                          "C0155664", "C0155665", "C0155668",
                                                                          "C0155642", "C0348591", "C0302375",
                                                                          "C0302376", "C0262565", "C0949079",
                                                                          "C1142565", "C0948864", "C0546119",
                                                                          "C0865592", "C0746727", "C0865596",
                                                                          "C0494580", "C0542269", "C2349195",
                                                                          "C0278960", "C0264706", "C0264707",
                                                                          "C0264704", "C0264705", "C0264703",
                                                                          "C0155638", "C0155637", "C0155636",
                                                                          "C0155634", "C0155633", "C0155632",
                                                                          "C0155631", "C0155630", "C0746710",
                                                                          "C0262567", "C0865697", "C0865699",
                                                                          "C0865698", "C0494577", "C1299620",
                                                                          "C1112769", "C1112662", "C1112663",
                                                                          "C0948865", "C1142433", "C0948866",
                                                                          "C0348864", "C0856742", "C0281915",
                                                                          "C0155646", "C0155647", "C0877768",
                                                                          "C0262563", "C0262564", "C0155643",
                                                                          "C0155640", "C0155641", "C0262568",
                                                                          "C0155648", "C0155649", "C0264674",
                                                                          "C0340319", "C1386160", "C0340312",
                                                                          "C0865603", "C0155659", "C0155658",
                                                                          "C0865593", "C0155650", "C0155653",
                                                                          "C0155652", "C0155655", "C0155654",
                                                                          "C0155657", "C0340293", "C0861151",
                                                                          "C1142184", "C1142185"]))
    angina_phrase_results = query_patients(es,
                                           populate_query_using_concepts("History_of_Past_Illness.anns.features.inst",
                                                                         ["C0002962", "C0858277", "C0235467",
                                                                          "C0002963", "C2024883", "C0152172",
                                                                          "C0948698", "C3805197", "C0859932",
                                                                          "C0541777", "C0577698", "C0859924",
                                                                          "C0340288", "C0206064"]))
    print 'query ended'
    s = ''
    for p in pids:
        r = [p]
        r.append('1') if p in hyper_phrase_results else r.append('0')
        r.append('1') if p in af_phrase_results else r.append('0')
        r.append('1') if p in cv_phrase_results else r.append('0')
        r.append('1') if p in angina_phrase_results else r.append('0')
        s += '\t'.join(r) + '\n'
    # print s
    # utils.save_string(s, '../resources/af_phrase_results.txt')
    utils.save_string(s, '../resources/af_semantic_results.txt')


def do_export_smp():
    corpus_mapping = load_corpus_to_FHIR_mapping('../resources/wrappers/mimic_FHIR_discharge_summary_map.tsv')
    smp_export('29463', es, corpus_mapping,
               '/Users/honghan.wu/Documents/UoE/semehr-usecase/mimic_structured_medical_profiles')


if __name__ == "__main__":
    # random_select_mimic_patients()
    es_setting = {
        'es_host': '10.200.102.23',
        'es_index': 'mimic',
        'es_doc_type': 'eprdoc',
        'es_concept_type': 'ctx_concept',
        'es_patient_type': 'patient'
    }
    es = SemEHRES.get_instance_by_setting(es_setting['es_host'],
                                          es_setting['es_index'],
                                          es_setting['es_doc_type'],
                                          es_setting['es_concept_type'],
                                          es_setting['es_patient_type'])

    # mimic_struct_extract_exp(es)
    do_export_smp()