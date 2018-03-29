from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions, helpers, TransportError
from datetime import timedelta, datetime
import utils

_es_host = '10.200.102.23'
_es_index = 'mimic'
_doc_type = 'eprdoc'
_concept_type = 'ctx_concept'
_patient_type = 'patient'
_es_instance = None
_page_size = 200


class SemEHRES(object):
    def __init__(self, es_host, es_index, doc_type, concept_type, patient_type):
        self._host = es_host
        self._es_instance = Elasticsearch([self._host], verify_certs=False)
        self._index = es_index
        self._doc_type = doc_type
        self._patient_type = patient_type
        self._concept_type = concept_type
        self._customise_settings = None

    def search_patient(self, q):
        patients = []
        need_next_query = True
        offset = 0
        while need_next_query:
            query = {"match": {"_all": q}} if len(q) > 0 else {"match_all": {}}
            results = self._es_instance.search(self._index, self._patient_type, {"query": query,
                                                                                 "from": offset,
                                                                                 "size": _page_size})
            total = results['hits']['total']
            for p in results['hits']['hits']:
                patients.append(p)
            offset += len(results['hits']['hits'])
            if offset >= total:
                need_next_query = False
        return patients

    @property
    def patient_type(self):
        return self._patient_type

    @property
    def doc_type(self):
        return self._doc_type

    @property
    def concept_type(self):
        return self._concept_type

    @staticmethod
    def do_collect_ids(d, container):
        container.append(d['_id'])

    def search_by_scroll(self, q, doc_type, collection_func=do_collect_ids):
        print q
        scroll_obj = self.scroll(q, doc_type, size=300)
        container = []
        utils.multi_thread_tasking_it(scroll_obj, 1, collection_func, args=[container])
        return container

    def get_contexted_concepts(self, concept):
        results = self._es_instance.search(self._index, self._concept_type, {"query": {"match": {"_all": concept}},
                                                                             "size": 2000
                                                                             })
        cc_to_ctx = {}
        for cc in results['hits']['hits']:
            d = cc['_source']
            cid = cc['_id']
            if d['experiencer'] == 'Other':
                cc_to_ctx[cid] = 'Other'
            elif d['temporality'] == 'historical':
                cc_to_ctx[cid] = 'historical'
            elif d['temporality'] == 'hypothetical':
                cc_to_ctx[cid] = 'hypothetical'
            elif d['negation'] == 'Negated':
                cc_to_ctx[cid] = 'Negated'
            else:
                cc_to_ctx[cid] = 'positive'
        return cc_to_ctx

    def summary_patients_by_concepts(self, concepts,
                                     filter_func=None, args=[], patient_filters=None,
                                     data_collection_func=None):
        cc_to_ctx = {}
        for t in concepts:
            cc_to_ctx.update(self.get_contexted_concepts(t))
        # print cc_to_ctx
        patients = self.search_patient(' '.join(concepts))
        results = []
        valid_docs = set()
        for p in patients:
            if patient_filters is not None and p['_id'] not in patient_filters:
                continue
            sp = {'id': p['_id'], 'all': 0}
            for ann in p['_source']['anns']:
                if ann['contexted_concept'] in cc_to_ctx:
                    if filter_func is not None:
                        # do filter, if filter function returns false, skip it
                        if not filter_func(*tuple(args + [ann, p])):
                            continue
                    valid_docs.add(ann['appearances'][0]['eprid'])
                    t = cc_to_ctx[ann['contexted_concept']]
                    sp[t] = 1 if t not in sp else sp[t] + 1
                    sp['all'] += 1

                    if data_collection_func is not None:
                        data_collection_func(*tuple(args + [ann, sp, t]))
            results.append(sp)
        return results, list(valid_docs)

    def get_doc_detail(self, doc_id, doc_type=None):
        doc_type = self._doc_type if doc_type is None else doc_type
        try:
            es_doc = self._es_instance.get(self._index, doc_id, doc_type=doc_type)
            if es_doc is not None:
                return es_doc['_source']
            else:
                return None
        except Exception:
            return None

    def search(self, entity, q, offset=0, size=10, include_fields=None):
        query = {"query": {"match": {"_all": q}},
                 "from": offset,
                 "size": size}
        if include_fields is not None:
            query['_source'] = {
                "includes": include_fields
            }
        print query
        results = self._es_instance.search(index=self._index, doc_type=entity, body=query)
        return results['hits']['total'], results['hits']['hits']

    def scroll(self, q, entity, size=100, include_fields=None, q_obj=None):
        query = {"query": {"match": {"_all": q}},
                 "size": size}
        if q_obj is not None:
            query = {
                "query": q_obj,
                "size": size
            }
        if include_fields is not None:
            query['_source'] = {
                "includes": include_fields
            }
        query['sort'] = '_doc'
        return helpers.scan(self._es_instance, query,
                            size=size, scroll='10m', index=self._index, doc_type=entity)

    def index_med_profile(self, doc_type, data, patient_id):
        self._es_instance.index(index=self._index, doc_type=doc_type, body=data, id=str(patient_id), timeout='30s')

    @staticmethod
    def get_instance():
        global _es_instance
        if _es_instance is None:
            _es_instance = SemEHRES(_es_host, _es_index, _doc_type, _concept_type, _patient_type)
        return _es_instance


    @staticmethod
    def get_instance_by_setting(es_host, es_index, es_doc_type, es_concept_type, es_patient_type):
        global _es_instance
        if _es_instance is None:
            _es_instance = SemEHRES(es_host, es_index, es_doc_type, es_concept_type, es_patient_type)
        return _es_instance

    @staticmethod
    def get_instance_by_setting_file(setting_file_path):
        setting = utils.load_json_data(setting_file_path)
        return SemEHRES.get_instance_by_setting(setting['es_host'], setting['es_index'],
                                                setting['es_doc_type'], setting['es_concept_type'],
                                                setting['es_patient_type'])


if __name__ == "__main__":
    es = SemEHRES.get_instance_by_setting_file('../index_settings/sem_idx_setting.json')
    # print es.get_doc_detail('1044334459', 'docs')
    # print es.search('docs', 'ward')
    try:
        print es.search('eprdoc', 'ward')
    except TransportError as terr:
        print terr.info