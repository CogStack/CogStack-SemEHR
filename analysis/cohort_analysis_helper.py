from semquery import SemEHRES
import utils
import json


def get_doc_by_id(p, doc_id):
    for d in p['_source']['articles']:
        if d['erpid'] == doc_id:
            return d['fulltext']
    return None


def query_doc_anns(es, concepts, skip_terms):
    patients = es.search_all(" ".join(concepts), es.patient_type, include_fields=['anns', 'articles'])
    print len(patients)
    doc_anns = {}
    for p in patients:
        docs = {}
        for ann in p['_source']['anns']:
            if ann['CUI'] in concepts:
                doc_id = ann['appearances'][0]['eprid']
                doc = get_doc_by_id(p, doc_id) if doc_id not in docs else docs[doc_id]
                if doc is None:
                    continue
                docs[doc_id] = doc
                string_orig = doc[ann['appearances'][0]['offset_start']:ann['appearances'][0]['offset_end']]
                if string_orig not in skip_terms:
                    a = {'s': ann['appearances'][0]['offset_start'], 'e': ann['appearances'][0]['offset_end'],
                         'contexted_concept': ann['contexted_concept']}
                    if doc_id not in doc_anns:
                        doc_anns[doc_id] = {'doc_id': doc_id, 'pid': p['_id'],
                                              "anns": [], 'text': doc}
                    doc_anns[doc_id]['anns'] += [a]
    print json.dumps(doc_anns)
    return doc_anns


if __name__ == "__main__":
    es = SemEHRES.get_instance_by_setting_file('../index_settings/sem_idx_setting.json')
    # print es.patient_type
    # print es.search_all('discuss', es.patient_type)
    # ["C0013987", "C0154409", "C0038050"]
    query_doc_anns(es, ["C0154409"], ["major depressive disorder, recurrent"])
