import utils
import json
import re
from semquery import SemEHRES


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


def do_query_analysis(d, container):
    container.append([s['section'] for s in parse_summary_structure(d['_source']['fulltext'])])


def analyse_discharge_summaries(es, q):
    """
    iterate all discharge summaries and create the section dictionary for
    the corpus (EHR system)
    :param es:
    :param q:
    :return:
    """
    scroll_obj = es.scroll(q, 'eprdoc', include_fields=['fulltext'], size=500)
    container = []
    utils.multi_thread_tasking_it(scroll_obj, 10, do_query_analysis, args=[container])
    print 'search finished. merging sections...'
    sec_freq = {}
    for ss in container:
        for s in ss:
            sec_freq[s] = 1 if s not in sec_freq else 1 + sec_freq[s]
    utils.save_json_array(sec_freq, '../resources/wrappers/mimic_section_freqs.json')
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
            print FHIR_Sec, s['pos']
    if len(sections) > 0:
        put_anns_into_section(full_text, prev_pos, prev_sec, len(full_text), anns, checked_ann_idx, structured_summary,
                              original_sec=prev_orig_sec)
    print json.dumps(structured_summary)


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

if __name__ == "__main__":
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
    doc = es.get_doc_detail('7175')
    print json.dumps(doc)
    # print '*' * 100
    # parse_summary_structure(doc['fulltext'])
    # analyse_discharge_summaries(es, 'docType:discharge summary')
    # container = []
    # do_query_analysis({'es':es,
    #                    'q': 'docType:discharge summary',
    #                    'offset': 0,
    #                    'last_offset': 10,
    #                    'page_size': 100}, container)
    # print json.dumps(container)
    # select_section_headers('../resources/wrappers/mimic_section_freqs.json')
    corpus_mapping = load_corpus_to_FHIR_mapping('../resources/wrappers/mimic_FHIR_discharge_summary_map.tsv')
    parse_discharge_summary(doc['fulltext'], doc['anns'], corpus_mapping)
