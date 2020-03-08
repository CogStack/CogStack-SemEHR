import utils
import json
import re
from elasticsearch import Elasticsearch
from semquery import SemEHRES
from os.path import isfile, join
from os import listdir



class AnonymiseRule(object):
    def __init__(self, rule_file):
        self._rules = utils.load_json_data(rule_file)

    @staticmethod
    def rul_extraction(full_text, re_objs):
        results = []
        for ro in re_objs:
            if 'disabled' in ro and ro['disabled']:
                continue
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

    def do_letter_parsing(self, full_text):
        re_exps = self._rules
        results = []
        header_pos = -1
        tail_pos = -1
        header_result = self.rul_extraction(full_text, [re_exps['letter_header_splitter']])
        tail_result = self.rul_extraction(full_text, [re_exps['letter_end_splitter']])
        results += header_result
        if len(header_result) > 0:
            header_pos = header_result[0]['pos'][0]
            header_text = full_text[:header_pos]
            phone_results = self.rul_extraction(header_text, re_exps['phone'])
            dr_results = self.rul_extraction(header_text, [re_exps['doctor']])
            results += phone_results
            results += dr_results
        if len(tail_result) > 0:
            tail_pos = tail_result[0]['pos'][1]
            tail_text = full_text[tail_pos:]
            for sent_type in re_exps['sent_rules']:
                results += self.rul_extraction(tail_text, re_exps[sent_type])
            # phone_results = self.rul_extraction(tail_text, re_exps['phone'])
            # dr_results = self.rul_extraction(tail_text, re_exps['doctor'])
            # addr_results = self.rul_extraction(tail_text, re_exps['address'])
            # addr_results += self.rul_extraction(tail_text, re_exps['clinic'])
            # addr_results += self.rul_extraction(tail_text, re_exps['assistant'])
            # # print tail_text
            # # print 'addr matched results [%s]' % addr_results
            # results += dr_results
            # results += phone_results
            # results += addr_results
        return results, header_pos, tail_pos

    def do_full_text_parsing(self, full_text):
        re_exps = self._rules
        matched_rets = []
        for st in re_exps['sent_rules']:
            rules = re_exps['sent_rules'][st]
            matched_rets += self.rul_extraction(full_text, rules if type(rules) is list else [rules])
        return matched_rets, 0, 0

    @staticmethod
    def do_replace(text, pos, sent_text, replace_char='x'):
        return text[:pos] + re.sub(r'[^\n\s]', 'x', sent_text) + text[pos+len(sent_text):]


def anonymise_doc(doc_id, text, failed_docs, anonymis_inst, sent_container):
    """
    anonymise a document
    :param doc_id:
    :param text:
    :param failed_docs:
    :param anonymis_inst: anonymise_rule instance
    :return:
    """
    # rets = do_letter_parsing(text)
    rets = anonymis_inst.do_full_text_parsing(text)
    if rets[1] < 0 or rets[2] < 0:
        failed_docs.append(doc_id)
        print '````````````` %s failed' % doc_id
        return None, None
    else:
        sen_data = rets[0]
        # print 'sentdata : [%s]' % sen_data
        anonymised_text = text
        for d in sen_data:
            if 'name' in d['attrs']:
                print 'removing %s [%s] ' % (d['attrs']['name'], d['type'])
                if is_valid_place_holder(d['attrs']['name']):
                    anonymised_text = AnonymiseRule.do_replace(anonymised_text, d['pos'][0] + d['attrs']['full_match'].find(d['attrs']['name']), d['attrs']['name'])
                    # 'x' * len(d['attrs']['name']))
                sent_container.append({'type': d['type'], 'sent': d['attrs']['name']})
            if 'number' in d['attrs']:
                print 'removing %s ' % d['attrs']['number']
                if is_valid_place_holder(d['attrs']['number']):
                    anonymised_text = AnonymiseRule.do_replace(anonymised_text, d['pos'][0], d['attrs']['number'])
                sent_container.append({'type': d['type'], 'sent': d['attrs']['number']})
        return anonymised_text, sen_data


def do_doc_anonymisation(doc, writing_es, writing_index_name, writing_doc_type,
                         full_text_field, container, failed_docs, anonymis_inst):
    print '======working on %s' % doc['_id']
    anonymised_text, sen_data = anonymise_doc(doc['_id'], doc['_source'][full_text_field], failed_docs, anonymis_inst)
    if anonymised_text is not None:
        data = doc['_source']
        data[full_text_field] = anonymised_text
        writing_es.index(index=writing_index_name, doc_type=writing_doc_type,
                         body=data, id=doc['_id'], timeout='30s')
        container += sen_data
        print '*******doc %s indexed' % doc['_id']


def is_valid_place_holder(s):
    return len(s) >= 2


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


def es_anonymisation():
    """
    query es index to anonymise texts
    :return:
    """
    es = init_es_inst()
    writing_es_host = "https://:1@IPADDRESS"
    writing_index_name = "index"
    writing_doc_type = "docs"
    doc_type = "docs"
    full_text_field = "field"
    parse_es_docs(es, 'description:"letter"', writing_es_host, writing_index_name, writing_doc_type, doc_type,
                  full_text_field)


def dir_anonymisation(folder, rule_file):
    anonymis_inst = AnonymiseRule(rule_file)
    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]
    container = []
    sent_data = []
    for f in onlyfiles:
        text = utils.read_text_file_as_string(join(folder, f))
        print anonymise_doc(f, text, container, anonymis_inst, sent_data)


def wrap_anonymise_doc(text, failed_docs, anonymis_inst, sent_container):
    anonymised_text, sen_data = anonymise_doc('id', text, failed_docs, anonymis_inst, sent_container)
    print(anonymised_text)


def mimic_anonymisation(single_file, rule_file):
    doc = utils.read_text_file_as_string(single_file)
    arr = re.split(r'START\_OF\_RECORD=\d+\|\|\|\|\d+\|\|\|\|\r{0,1}\n', doc)
    i = 0
    texts = []
    for t in arr:
        texts.append(t.replace('||||END_OF_RECORD\n', ''))

    anonymis_inst = AnonymiseRule(rule_file)
    failed_docs = []
    sent_data = []
    utils.multi_thread_tasking(texts, 1, wrap_anonymise_doc, args=[failed_docs, anonymis_inst, sent_data])
    t2sent = {}
    for s in sent_data:
        if s['type'] not in t2sent:
            t2sent[s['type']] = []
        t2sent[s['type']].append(s['sent'])
    for t in t2sent:
        t2sent[t] = list(set(t2sent[t]))
        print('%s\n======\n%s\n\n' % (t, '\n'.join(t2sent[t])))


if __name__ == "__main__":
    # dir_anonymisation('C:/Users/hwu33/Downloads/research_datasets/deidentified-medical-text-1.0/files',
    #                   './conf/anonymise_rules.json')
    mimic_anonymisation('C:/Users/hwu33/Downloads/research_datasets/deidentified-medical-text-1.0/id.text',
                        './conf/anonymise_rules.json')
