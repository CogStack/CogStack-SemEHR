from semquery import SemEHRES
import utils
import json


def get_doc_by_id(p, doc_id):
    for d in p['articles']:
        if d['erpid'] == doc_id:
            return d['fulltext']
    return None


def retrieve_doc_by_id(doc_id, es, index_name='epr_documents', full_text_field='body_analysed'):
    d = es.get_doc_detail(doc_id=doc_id, index_name=index_name)
    if d is not None and full_text_field in d:
        return d[full_text_field]
    else:
        return None


def collect_patient_docs(po, es, concepts, skip_terms, container, filter_obj=None, doc_filter_function=None):
    docs = {}
    pid = po['_id']
    p = es.get_doc_detail(pid, doc_type=es.patient_type)
    doc_anns = {}
    for ann in p['anns']:
        if ann['cui'] in concepts:
            doc_id = ann['eprid']
            doc = retrieve_doc_by_id(doc_id, es) if doc_id not in docs else docs[doc_id]
            if doc is None:
                continue
            if doc_filter_function is not None:
                if doc_filter_function(filter_obj, doc_id, pid):
                    continue
            docs[doc_id] = doc
            string_orig = doc[ann['start']:ann['end']]
            if string_orig not in skip_terms:
                a = {'s': ann['start'], 'e': ann['end'],
                     'contexted_concept': ann['contexted_concept'],
                     'inst': ann['cui']}
                if doc_id not in doc_anns:
                    doc_anns[doc_id] = {'doc_id': doc_id, 'pid': pid,
                                        "anns": [], 'text': doc}
                doc_anns[doc_id]['anns'] += [a]
    container.append(doc_anns)


def query_doc_anns(es, concepts, skip_terms, retained_patients_filter=None, filter_obj=None, doc_filter_function=None):
    patients = es.search_by_scroll(" ".join(concepts), es.patient_type, collection_func=lambda d, c: c.append(d))
    print '%s patients matched' % len(patients)
    if retained_patients_filter is not None:
        retained = []
        for po in patients:
            if po['_id'] in retained_patients_filter:
                retained.append(po)
        patients = retained
        print 'patients filtered to size %s' % len(patients)
    doc_anns = {}
    container = []
    utils.multi_thread_tasking(patients, 40, collect_patient_docs,
                               args=[es, concepts, skip_terms, container, filter_obj, doc_filter_function])
    print 'data collected, merging...'
    for d in container:
        doc_anns.update(d)
    print 'merged dic size %s' % len(doc_anns)
    return doc_anns


def query_collect_patient_docs(po, des, es_search, patiet_id_field, container, filter_obj=None, doc_filter_function=None):
    """
    collect docs using filtering functions
    :param po:
    :param des:
    :param es_search:
    :param patiet_id_field:
    :param container:
    :param filter_obj:
    :param doc_filter_function:
    :return:
    """
    pid = po['_id']
    docs = des.search_by_scroll(patiet_id_field + ":" + pid
                                + " AND (" + " ".join(es_search) + ")",
                                des.doc_type, collection_func=lambda d, c: c.append(d))
    matched_docs = []
    for d in docs:
        doc_id = d['_id']
        if doc_filter_function is not None:
            if doc_filter_function(filter_obj, doc_id, pid):
                continue
        matched_docs.append(doc_id)
    container.append({'pid': pid, 'docs': matched_docs})


def query_doc_by_search(es, doc_es, es_search, patiet_id_field, retained_patients_filter=None, filter_obj=None,
                        doc_filter_function=None):
    """
    get number of mentions by elasticsearch queries instead of NLP results
    :param es:
    :param doc_es:
    :param es_search:
    :param patiet_id_field:
    :param retained_patients_filter:
    :param filter_obj:
    :param doc_filter_function:
    :return:
    """
    patients = es.search_by_scroll(" ".join(es_search), es.patient_type, collection_func=lambda d, c: c.append(d))
    print '%s patients matched' % len(patients)
    if retained_patients_filter is not None:
        retained = []
        for po in patients:
            if po['_id'] in retained_patients_filter:
                retained.append(po)
        patients = retained
        print 'patients filtered to size %s' % len(patients)
    container = []
    utils.multi_thread_tasking(patients, 40, query_collect_patient_docs,
                               args=[doc_es, es_search, patiet_id_field, container, filter_obj, doc_filter_function])
    return container


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
