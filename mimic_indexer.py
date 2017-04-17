from entity_centric_es import EntityCentricES
import json
from mimicdao import get_mimic_doc_by_id
import utils
from os.path import isfile, join
from os import listdir


_f_yodie_anns = './sample_anns/'


def do_index_mimic(line, es, patients):
    ann_data = json.loads(line)
    doc_id = ann_data['docId']
    doc_obj = get_mimic_doc_by_id(doc_id)
    if doc_obj is not None and len(doc_obj) > 0:
        doc_obj = doc_obj[0]
        full_text = doc_obj['text']
        print '%s indexed' % doc_id
        es.index_document({'eprid': doc_id,
                           # 'date': doc_obj['Date'],
                           'patientId': str(doc_obj['subject_id']),
                           'stayId': str(doc_obj['hadm_id']),
                           'charttime': doc_obj['charttime'],
                           'chartdate': doc_obj['chartdate'],
                           'fulltext': full_text,
                           'anns': ann_data['annotations'][0]},
                          doc_id)
        es.index_entity_data_v2('', '', anns=ann_data['annotations'][0])
        patients.append(doc_obj['subject_id'])
    else:
        print '[ERROR] doc %s not found' % doc_id


def index_mimic_notes():
    es = EntityCentricES.get_instance('./pubmed_test/es_mimic_setting.json')
    ann_files = [f for f in listdir(_f_yodie_anns) if isfile(join(_f_yodie_anns, f))]
    patients = []
    for ann in ann_files:
        print 'indexing %s ...' % ann
        utils.multi_thread_large_file_tasking(join(_f_yodie_anns, ann), 20, do_index_mimic,
                                              args=[es, patients])
    print 'full text and annotations done.'
    patients = list(set(patients))
    index_patients(patients, es)


def do_index_patient(patient, es):
    es.query_entity_to_index(patient)


def index_patients(patients, es):
    print 'indexing %s patients...' % len(patients)
    utils.multi_thread_tasking(patients, 10, do_index_patient, args=[es])
    print 'patient indexing done'


if __name__ == "__main__":
    index_mimic_notes()
