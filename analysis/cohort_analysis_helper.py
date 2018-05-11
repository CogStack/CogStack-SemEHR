from semquery import SemEHRES
import utils
import json


def get_doc_by_id(p, doc_id):
    for d in p['articles']:
        if d['erpid'] == doc_id:
            return d['fulltext']
    return None


def collect_patient_docs(po, es, concepts, skip_terms, container):
    docs = {}
    pid = po['_id']
    p = es.get_doc_detail(pid, doc_type=es.patient_type)
    doc_anns = {}
    for ann in p['anns']:
        if ann['CUI'] in concepts:
            doc_id = ann['appearances'][0]['eprid']
            doc = get_doc_by_id(p, doc_id) if doc_id not in docs else docs[doc_id]
            if doc is None:
                continue
            docs[doc_id] = doc
            string_orig = doc[ann['appearances'][0]['offset_start']:ann['appearances'][0]['offset_end']]
            if string_orig not in skip_terms:
                a = {'s': ann['appearances'][0]['offset_start'], 'e': ann['appearances'][0]['offset_end'],
                     'contexted_concept': ann['contexted_concept'],
                     'inst': ann['CUI']}
                if doc_id not in doc_anns:
                    doc_anns[doc_id] = {'doc_id': doc_id, 'pid': pid,
                                          "anns": [], 'text': doc}
                doc_anns[doc_id]['anns'] += [a]
    container.append(doc_anns)


def query_doc_anns(es, concepts, skip_terms, retained_patients_filter=None):
    patients = es.search_by_scroll(" ".join(concepts), es.patient_type, collection_func=lambda d, c: c.append(d))
    print '%s patients matched' % len(patients)
    if retained_patients_filter is not None:
        retained = []
        for po in patients:
            if po['_id'] in retained_patients_filter:
                retained.append(po)
        patients = retained
    doc_anns = {}
    container = []
    utils.multi_thread_tasking(patients, 40, collect_patient_docs,
                                   args=[es, concepts, skip_terms, container])
    print 'data collected, merging...'   
    for d in container:                  
        doc_anns.update(d)
    print 'merged dic size %s' % len(doc_anns)                            
    return doc_anns


def get_all_patient_ids():
    patients = es.search_by_scroll("*", es.patient_type)
    print 'total %s patients found' % len(patients)
    return get_all_patient_ids


if __name__ == "__main__":
    es = SemEHRES.get_instance_by_setting_file('../index_settings/sem_idx_setting.json')
    # print es.patient_type
    # print es.search_all('discuss', es.patient_type)
    # ["C0013987", "C0154409", "C0038050"]
    query_doc_anns(es, ["C0154409"], ["major depressive disorder, recurrent"])
    # get_all_patient_ids()
