import utils
import json
import re
from elasticsearch import Elasticsearch
from semquery import SemEHRES

re_exps = {
    'letter_header_splitter': {
        'pattern': r'^Dear\s+([A-Z]{1,}[A-Za-z\-])*\s+(.*)$',
        'flags': ['multiline'],
        'data_labels': ['title', 'name'],
        'data_type': 'letter receiver'
    },
    'letter_end_splitter': {
        'pattern': r'^(Yours){0,}\s+(sincerely|faithfully)\,{0,}$',
        'flags': ['multiline', 'ignorecase'],
        'data_type': 'letter end text'
    },
    'doctor': {
        'pattern': r' {0,}(Dr|Prof|Professor|Miss|Ms|Mr|Mrs)\.{0,1} +([A-Za-z\-]+( {0,}([A-Za-z\-]*)){0,1})',
        # r'^ {0,}(Dr|Prof|Professor|Miss|Ms|Mr|Mrs) +([A-Za-z]+ {0,}(.*))$',
        'flags': ['multiline', 'ignorecase'],
        'data_labels': ['title', 'name'],
        'data_type': 'doctor'
    },
    'clinic': [{
        'pattern': r'(([^\s]* {1,}){1,2})(Hospital|surgery|Prim Ctre|Clinic)',
        'flags': ['multiline', 'ignorecase'],
        'data_labels': ['name', 'inst_type'],
        'data_type': 'clinic'
    }
    ],
    'assistant': [{
        'pattern': r'(.*)(secretary|nurse)',
        'flags': ['multiline', 'ignorecase'],
        'data_labels': ['name', 'inst_type'],
        'data_type': 'assistant'
    }
    ],
    'phone': [{
        'pattern': r' {0,}(telephone|phone|tel no|Fax No|Fax number|Appointments|Facsimile|fax|tel)\.{0,1}\s{0,1}\:{0,1}\s+((\d{2,}( |\-){0,1}){1,}).*',
        'flags': ['multiline', 'ignorecase'],
        'data_labels': ['label', 'number'],
        'data_type': 'phone'
    },
        {
            'pattern': r'((secretary to|reception)[^\d]+((\d{2,}( |\-){0,1}){1,}))',
            'flags': ['multiline', 'ignorecase'],
            'data_labels': ['full', 'label', 'number'],
            'data_type': 'phone'
        },
        {
            'pattern': r'(\(\d+\) {0,}\d{2,}( |\-){0,}\d{2,})',
            'flags': ['multiline', 'ignorecase'],
            'data_labels': ['number'],
            'data_type': 'phone'
        }
    ],
    'address': [{
        'pattern': r'((.*\n){3}[^\n]{0,}(([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z]))))\s?[0-9][A-Za-z]{2})))',
        'flags': ['multiline', 'ignorecase'],
        'data_labels': ['name', 'partial', 'postcode'],
        'data_type': 'address'
    },
        {
            'pattern': r'(((flat|room)[^\n]+){0,}\d{1,}[^\n]+(([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z]))))\s?[0-9][A-Za-z]{2})))',
            'flags': ['multiline', 'ignorecase'],
            'data_labels': ['name', 'partial', 'postcode'],
            'data_type': 'address'
        }
    ]
}


def rul_extraction(full_text, re_objs):
    results = []
    for ro in re_objs:
        flag = 0
        if 'multiline' in ro['flags']:
            flag |= re.MULTILINE
        if 'ignorecase' in ro['flags']:
            flag |= re.IGNORECASE
        matches = re.finditer(ro['pattern'], full_text, flag)
        for m in matches:
            ret = {'type': ro['data_type'], 'attrs': {}}
            results.append(ret)
            ret['attrs']['full_match'] = m.group(0)
            ret['pos'] = m.span()
            i = 1
            if 'data_labels' in ro:
                for attr in ro['data_labels']:
                    ret['attrs'][attr] = m.group(i)
                    i += 1
    return results


def do_letter_parsing(full_text):
    results = []
    header_pos = -1
    tail_pos = -1
    header_result = rul_extraction(full_text, [re_exps['letter_header_splitter']])
    tail_result = rul_extraction(full_text, [re_exps['letter_end_splitter']])
    results += header_result
    if len(header_result) > 0:
        header_pos = header_result[0]['pos'][0]
        header_text = full_text[:header_pos]
        phone_results = rul_extraction(header_text, re_exps['phone'])
        dr_results = rul_extraction(header_text, [re_exps['doctor']])
        results += phone_results
        results += dr_results
    if len(tail_result) > 0:
        tail_pos = tail_result[0]['pos'][1]
        tail_text = full_text[tail_pos:]
        phone_results = rul_extraction(tail_text, re_exps['phone'])
        dr_results = rul_extraction(tail_text, [re_exps['doctor']])
        addr_results = rul_extraction(tail_text, re_exps['address'])
        addr_results += rul_extraction(tail_text, re_exps['clinic'])
        addr_results += rul_extraction(tail_text, re_exps['assistant'])
        # print tail_text
        # print 'addr matched results [%s]' % addr_results
        results += dr_results
        results += phone_results
        results += addr_results
    return results, header_pos, tail_pos


def do_full_text_parsing(full_text):
    matched_rets = []
    for ptn in re_exps:
        matched_rets += rul_extraction(full_text, re_exps[ptn] if type(re_exps[ptn]) is list else [re_exps[ptn]])
    return matched_rets, 0, 0


def do_doc_anonymisation(doc, writing_es, writing_index_name, writing_doc_type,
                         full_text_field, container, failed_docs):
    print '======working on %s' % doc['_id']
    text = doc['_source'][full_text_field]
    # rets = do_letter_parsing(text)
    rets = do_full_text_parsing(text)
    if rets[1] < 0 or rets[2] < 0:
        failed_docs.append(doc['_id'])
        print '````````````` %s failed' % doc['_id']
    else:
        data = doc['_source']
        sen_data = rets[0]
        # print 'sentdata : [%s]' % sen_data
        anonymised_text = text
        for d in sen_data:
            if 'name' in d['attrs']:
                print 'removing %s ' % d['attrs']['name']
                if is_valid_place_holder(d['attrs']['name']):
                    anonymised_text = anonymised_text.replace(d['attrs']['name'],
                                                              re.sub(r'[^\n]', 'x', d['attrs']['name']))
                    # 'x' * len(d['attrs']['name']))
            if 'number' in d['attrs']:
                print 'removing %s ' % d['attrs']['number']
                if is_valid_place_holder(d['attrs']['number']):
                    anonymised_text = anonymised_text.replace(d['attrs']['number'], 'x' * len(d['attrs']['number']))
        data[full_text_field] = anonymised_text
        writing_es.index(index=writing_index_name, doc_type=writing_doc_type,
                         body=data, id=doc['_id'], timeout='30s')
        container += sen_data
        print '*******doc %s indexed' % doc['_id']


def is_valid_place_holder(s):
    return len(s) > 2


def init_es_inst():
    es_setting = {
        'es_host': 'https://@/',
        'es_index': 'index',
        'es_doc_type': '',
        'es_concept_type': '',
        'es_patient_type': ''
    }
    es = SemEHRES.get_instance_by_setting(es_setting['es_host'],
                                          es_setting['es_index'],
                                          es_setting['es_doc_type'],
                                          es_setting['es_concept_type'],
                                          es_setting['es_patient_type'])
    return es


def parse_es_docs(es, q,
                  writing_es_host, writing_index_name, writing_doc_type,
                  doc_type='eprdoc',
                  full_text_field='fulltext',
                  output_file='../resources/wrappers/sen_data_extracted.json',
                  failed_docs_file='../resources/wrappers/sen_failed_docs.json', ):
    writing_es = Elasticsearch([writing_es_host], verify_certs=False)
    # scroll_obj = es.scroll(q, doc_type, include_fields=[full_text_field], size=500)
    ret_count, docs = es.search(doc_type, q, offset=0, size=30)
    container = []
    failed_docs = []
    print 'anonymising... %s, %s' % (len(docs), ','.join([d['_id'] for d in docs]))
    utils.multi_thread_tasking_it(docs, 1, do_doc_anonymisation,
                                  args=[writing_es, writing_index_name, writing_doc_type, full_text_field, container,
                                        failed_docs])
    print 'search finished. merging sections...'
    utils.save_json_array(container, output_file)
    utils.save_json_array(failed_docs_file, failed_docs_file)
    print 'done'


if __name__ == "__main__":
    es = init_es_inst()
    writing_es_host = "https://:1@IPADDRESS"
    writing_index_name = "index"
    writing_doc_type = "docs"
    doc_type = "docs"
    full_text_field = "field"
    parse_es_docs(es, 'description:"letter"', writing_es_host, writing_index_name, writing_doc_type, doc_type,
                  full_text_field)
