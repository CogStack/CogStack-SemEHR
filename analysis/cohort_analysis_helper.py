from semquery import SemEHRES
import utils
import json


def query_doc_anns(es, concepts, skip_terms):
    docs = es.search_by_scroll(" ".join(concepts), es.doc_type, collection_func=lambda d, c: c.append(d))
    doc_anns = {}
    for d in docs:
        v_anns = []
        for ann in d['_source']['anns']:
            if ann['features']['inst'] in concepts:
                if ann['features']['string_orig'] not in skip_terms:
                    a = {'s': ann['startNode']['offset'], 'e': ann['endNode']['offset'],
                         'features': ann['features']}
                    if d['_id'] not in doc_anns:
                        doc_anns[d['_id']] = {'doc_id': d['_id'], 'pid': d['_source']['patientId'],
                                              "anns": [], 'text': d['_source']['fulltext']}
                    doc_anns[d['_id']]['anns'] += [a]
    print json.dumps(doc_anns)


if __name__ == "__main__":
    es = SemEHRES.get_instance_by_setting_file('../index_settings/sem_idx_setting.json')
    print es.search_by_scroll('14479', es.patient_type)
    query_doc_anns(es, ["C0154409", "C0038050"], ["major depressive disorder, recurrent"])
