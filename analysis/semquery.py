from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions
from datetime import timedelta, datetime
import utils

_es_host = 'localhost'
_es_index = 'addiction_20k'
_doc_type = 'eprdoc'
_concept_type = 'ctx_concept'
_patient_type = 'patient'
_es_instance = None
_page_size = 200


class SemEHRES(object):
    def __init__(self, es_host, es_index, doc_type, concept_type, patient_type):
        self._host = es_host
        self._es_instance = Elasticsearch([self._host])
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

    def get_contexted_concepts(self, concept):
        results = self._es_instance.search(self._index, self._concept_type, {"query": {"match": {"_all": concept}},
                                                                             "size": 100
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
        print len(cc_to_ctx)
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

    @staticmethod
    def get_instance():
        global _es_instance
        if _es_instance is None:
            _es_instance = SemEHRES(_es_host, _es_index, _doc_type, _concept_type, _patient_type)
        return _es_instance


