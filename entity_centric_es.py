from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions
import hashlib
import utils
import json
from os.path import join, isfile
from os import listdir
from cohortanalysis import load_all_docs
from datetime import datetime
import sys

_ann_doc_type = 'ann_insts'


class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)


class EntityCentricES(object):

    def __init__(self, es_host):
        self._host = es_host
        self._es_instance = Elasticsearch([es_host])
        self._index = 'semehr'
        self._concept_doc_type = 'ctx_concept'
        self._entity_doc_type = 'user'
        self._doc_doc_type = 'doc'
        self._customise_settings = None

    @property
    def index_name(self):
        return self._index

    @index_name.setter
    def index_name(self, value):
        self._index = value

    @property
    def concept_doc_type(self):
        return self._concept_doc_type

    @concept_doc_type.setter
    def concept_doc_type(self, value):
        self._concept_doc_type = value

    @property
    def entity_doc_type(self):
        return self._entity_doc_type

    @entity_doc_type.setter
    def entity_doc_type(self, value):
        self._entity_doc_type = value

    @property
    def doc_doc_type(self):
        return self._doc_doc_type

    @doc_doc_type.setter
    def doc_doc_type(self, value):
        self._doc_doc_type = value

    @property
    def customise_settings(self):
        return self._customise_settings

    @customise_settings.setter
    def customise_settings(self, value):
        self._customise_settings = value

    def init_index(self, mapping):
        if self._es_instance.indices.exists(self.index_name):
            self._es_instance.indices.delete(self.index_name)
        self._es_instance.indices.create(self.index_name)
        for t in mapping:
            self._es_instance.indices.put_mapping(doc_type=t, body=mapping[t])

    def index_ctx_concept(self, ann, index_instance=False):
        data = {
            "doc": {
                "cui": ann['features']['inst'],
                "negation": ann['features']['Negation'],
                "experiencer": ann['features']['Experiencer'],
                "temporality": ann['features']['Temporality'],
                "prefLabel": ann['features']['PREF']
                # "vocabularies": ann['features']['VOCABS'],
                # "STY": ann['features']['STY']
            },
            "doc_as_upsert": True
        }
        ctx_id = EntityCentricES.get_ctx_concept_id(ann)
        # print json.dumps(data)
        self._es_instance.update(index=self.index_name, doc_type=self.concept_doc_type, id=ctx_id, body=data,
                                 retry_on_conflict=30, timeout='30s')
        if index_instance:
            ann['ctx_id'] = ctx_id
            self._es_instance.index(index=self.index_name, doc_type='concept_inst', body=ann, timeout='30s')

    def index_document(self, doc_obj, id):
        self._es_instance.index(index=self.index_name, doc_type=self.doc_doc_type, body=doc_obj, id=id, timeout='30s')
    
    def delete_index(self, doc_type):
        self._es_instance.delete(index=self.index_name, doc_type=doc_type)

    def index_entity_data(self, entity_id, doc_id, anns=None, article=None, doc_date=None):
        scripts = []
        data = {
            "params": {
            },
            "upsert": {
                "id": entity_id,
            }
        }
        if anns is not None:
            scripts.append("ctx._source.anns += anns")
            entity_anns = \
                [
                    {
                        "contexted_concept": EntityCentricES.get_ctx_concept_id(ann),
                        "CUI": ann['features']['inst'],
                        "appearances": [
                            {
                                "eprid": doc_id,
                                # "date": 0 if doc_date is None else doc_date,
                                "offset_start": int(ann['startNode']['offset']),
                                "offset_end": int(ann['endNode']['offset'])
                            }
                        ]
                    } for ann in anns
                    ]
            data['params']['anns'] = entity_anns
            data['upsert']['anns'] = entity_anns
            for ann in anns:
                self.index_ctx_concept(ann)
            print '[concepts] %s indexed' % len(anns)

        if article is not None:
            scripts.append("if (ctx._source.articles == null) " \
                           "{ ctx._source.articles = [article] } else " \
                           "{ ctx._source.articles = ctx._source.articles + article}")
            data['params']['article'] = article
            data['upsert']['articles'] = [article]

        data['script'] = ';'.join(scripts)

        # print json.dumps(data)
        # print 'patient %s updated' % entity_id
        self._es_instance.update(index=self.index_name, doc_type=self.entity_doc_type, id=entity_id, body=data)

    def index_anns(self, entity_id, doc_id, anns):
        if anns is not None:
            entity_anns = \
                [
                    {
                        "contexted_concept": EntityCentricES.get_ctx_concept_id(ann),
                        "CUI": ann['features']['inst'],
                        "appearances": [
                            {
                                "eprid": doc_id,
                                # "date": 0 if doc_date is None else doc_date,
                                "offset_start": int(ann['startNode']['offset']),
                                "offset_end": int(ann['endNode']['offset'])
                            }
                        ]
                    } for ann in anns
                ]
            data = {'patientId': entity_id, 'anns': entity_anns}
            self._es_instance.update(index=self.index_name, doc_type=_ann_doc_type, body=data)
            for ann in anns:
                self.index_ctx_concept(ann)
            print '[concepts] %s indexed' % len(anns)

    def query_to_index_entities(self, entity_id,
                                doc_es_inst, ft_index_name, ft_doc_type, ft_entity_field_id, ft_fulltext_field_id):
        """
        query the anns index and full text index to index the patient data
        :param entity_id:
        :param doc_es_inst:
        :param ft_index_name:
        :param ft_doc_type:
        :param ft_entity_field_id:
        :param ft_fulltext_field_id:
        :return:
        """
        ann_results = self._es_instance.search(index=self.index_name,
                                               doc_type=self.doc_doc_type,
                                               body={'query': {'term': {'eprid': entity_id}}})
        doc_results = doc_es_inst.search(index=ft_index_name,
                                         doc_type=ft_doc_type,
                                         body={'query': {'term': {ft_entity_field_id: entity_id}}})
        data = {
            "id": str(entity_id)
        }
        entity_anns = []
        articles = []
        for d in ann_results['hits']['hits']:
            if 'anns' in d['_source']:
                anns = d['_source']['anns']
                entity_anns += anns

        for d in doc_results['hits']['hits']:
            articles.append({'erpid': d['_id'], 'fulltext': d['_source'][ft_fulltext_field_id]})
        data['anns'] = entity_anns
        data['articles'] = articles
        self._es_instance.index(index=self.index_name, doc_type=self.entity_doc_type,
                                body=data, id=str(entity_id), timeout='30s')
        print 'patient %s indexed' % entity_id
    
    def query_entity_to_index(self, entity_id, entity_field_id='patientId'):
        results = self._es_instance.search(index=self.index_name,
                                           doc_type=self.doc_doc_type,
                                           body={'query': {'term': {entity_field_id: entity_id}}})
        data = {
            "id": str(entity_id)
        }
        entity_anns = []
        articles = []
        for d in results['hits']['hits']:
            articles.append({'erpid': d['_id'], 'fulltext': d['_source']['fulltext']})
            if 'anns' in d['_source']:
                anns = d['_source']['anns']
                entity_anns += \
                    [
                        {
                            "contexted_concept": EntityCentricES.get_ctx_concept_id(ann),
                            "CUI": ann['features']['inst'],
                            "appearances": [
                                {
                                    "eprid": d['_id'],
                                    #"date": 0 if doc_date is None else doc_date,
                                    "offset_start": int(ann['startNode']['offset']),
                                    "offset_end": int(ann['endNode']['offset'])
                                }
                            ]
                        } for ann in anns
                        ]
        data['anns'] = entity_anns
        data['articles'] = articles
        self._es_instance.index(index=self.index_name, doc_type=self.entity_doc_type, body=data, id=str(entity_id), timeout='30s')
        print 'patient %s indexed' % entity_id

    def index_entity_data_v2(self, entity_id, doc_id, anns=None, article=None, doc_date=None):        
        if anns is not None:
            for ann in anns:
                self.index_ctx_concept(ann)
            print '[concepts] %s indexed' % len(anns)

    @staticmethod
    def get_ctx_concept_id(ann):
        s = "%s_%s_%s_%s" % (ann['features']['inst'],
                             ann['features']['Negation'],
                             ann['features']['Experiencer'],
                             ann['features']['Temporality'])
        return hashlib.md5(s).hexdigest().upper()

    @staticmethod
    def get_instance(setting_file):
        setting = utils.load_json_data(setting_file)
        es = EntityCentricES(setting['es_host'])
        es.index_name = setting['index']
        es.concept_doc_type = setting['concept_doc_type']
        es.entity_doc_type = setting['entity_doc_type']
        if 'doc_doc_type' in setting and setting['doc_doc_type'] != '':
            es.doc_doc_type = setting['doc_doc_type']
        if 'customise_settings' in setting:
            es.customise_settings = setting['customise_settings']
        if setting['reset']:
            es.init_index(setting['mappings'])
        return es


def do_index_pubmed(line, es, pmcid_to_journal, full_text_path):
    ann_data = json.loads(line)
    pmcid = ann_data['docId']
    if pmcid in pmcid_to_journal:
        journal_name = pmcid_to_journal[pmcid]
        es.index_entity_data(hashlib.md5(journal_name).hexdigest().upper(),
                             pmcid, ann_data['annotations'][0],
                             {"pmcid:": pmcid,
                              "fulltext": utils.read_text_file_as_string(join(full_text_path, pmcid))
                              })


def do_index_pubmed_docs(doc_obj, es, full_text_path):
    if 'pmcid' in doc_obj:
        pmcid = doc_obj['pmcid']
        doc_obj['fulltext'] = utils.read_text_file_as_string(join(full_text_path, pmcid))
        es.index_document(doc_obj, pmcid)
        print 'doc %s indexed' % pmcid


def index_pubmed():
    es = EntityCentricES.get_instance('./pubmed_test/es_setting.json')
    doc_details = utils.load_json_data('./pubmed_test/pmc_docs.json')
    pmcid_to_journal = {}
    for d in doc_details:
        if 'pmcid' in d and 'journalTitle' in d:
            pmcid_to_journal[d['pmcid']] = d['journalTitle']
    # load anns
    # utils.multi_thread_large_file_tasking('./pubmed_test/test_anns.json', 10, do_index_pubmed,
    #                                       args=[es, pmcid_to_journal, './pubmed_test/fulltext'])
    utils.multi_thread_tasking(doc_details, 10, do_index_pubmed_docs,
                               args = [es, './pubmed_test/fulltext'])
    print 'done'


def do_index_100k_anns(line, es, doc_to_patient):
    ann_data = json.loads(line)
    doc_id = ann_data['docId']
    if doc_id in doc_to_patient:
        patient_id = doc_to_patient[doc_id]
        es.index_anns(patient_id,
                      doc_id, ann_data['annotations'][0])


def do_index_100k_patients(patient_id, es,
                           fulltext_es, ft_index_name, ft_doc_type, ft_entity_field, ft_fulltext_field):
    es.query_to_index_entities(patient_id, fulltext_es, ft_index_name, ft_doc_type, ft_entity_field, ft_fulltext_field)


def index_100k():
    f_patient_doc = ''
    f_yodie_anns = ''

    es = EntityCentricES.get_instance('./pubmed_test/es_100k_setting.json')
    es_epr_full_text = es.customise_settings['es_ft']
    ft_index_name = es.customise_settings['ft_index_name']
    ft_doc_type = es.customise_settings['ft_doc_type']
    ft_entity_field = es.customise_settings['ft_entity_field']
    ft_fulltext_field = es.customise_settings['ft_fulltext_field']

    lines = utils.read_text_file(f_patient_doc)
    doc_to_patient = {}
    patients = set()
    for l in lines:
        arr = l.split('\t')
        doc_to_patient[arr[1]] = arr[0]
        patients.add(arr[0])
    patients = list(patients)
    # epr full text index api
    es_full_text = Elasticsearch([es_epr_full_text], serializer=JSONSerializerPython2())
    es_full_text.get()
    utils.multi_thread_large_file_tasking(f_yodie_anns, 10, do_index_100k_anns,
                                          args=[es, doc_to_patient])
    print 'anns done, indexing patients...'
    utils.multi_thread_tasking(patients, 10, do_index_100k_patients,
                               args=[es, es_full_text,
                                     ft_index_name,
                                     ft_doc_type,
                                     ft_entity_field,
                                     ft_fulltext_field])
    print 'all done'


def load_doc_from_dir(folder, doc_id):
    doc_obj = utils.load_json_data(join(folder, doc_id + '.json'))
    doc_obj['TextContent'] = utils.read_text_file_as_string(join(folder, doc_id + '.txt'))
    return doc_obj


def do_index_cris(line, es, doc_to_patient, doc_dict, container):
    ann_data = json.loads(line)
    doc_id = ann_data['docId']    
    if doc_id in doc_to_patient:
        patient_id = doc_to_patient[doc_id]
        # doc_obj = get_doc_detail_by_id(doc_id)
        doc_obj = doc_dict[doc_id]
        if doc_obj is not None:
            # doc_obj = doc_obj[0] 
            full_text = doc_obj['TextContent'].decode('iso-8859-1').encode('utf-8')
            print '%s indexed' % doc_id
            es.index_document({'eprid': doc_id,
                               # 'date': doc_obj['Date'],
                               'patientId': str(doc_obj['BrcId']),
                               'src_table': doc_obj['src_table'],
                               'src_col': doc_obj['src_col'],
                               'fulltext': full_text,
                               'anns': ann_data['annotations'][0]}, 
                               doc_id)
            es.index_entity_data_v2(patient_id,
                                 doc_id, ann_data['annotations'][0],
                                 {
                                     "eprid:": doc_id,
                                     "fulltext": full_text
                                 },
                                 doc_date=doc_obj['Date']) #doc_obj['Date'])
        else:
            print '[ERROR] %s full text not found' % doc_id
        container.append(doc_id)
    else:
        print '[ERROR] %s not found in dic' % doc_id


def index_cris_cohort():
    f_patient_doc = './hepc_pos_doc_brcid.txt'
    f_yodie_anns = 'U:/kconnect/hepc_output/'
    print 'loading all docs at a time...'
    docs = load_all_docs()
    print 'docs read'
    doc_dict = {}
    for d in docs:
        doc_dict[d['CN_Doc_ID']] = d

    es = EntityCentricES.get_instance('./pubmed_test/es_cris_setting.json')
    lines = utils.read_text_file(f_patient_doc, encoding='utf-8-sig')
    doc_to_patient = {}
    for l in lines:
        arr = l.split('\t')
        doc_to_patient[arr[1]] = arr[0]
    container = []
    ann_files = [f for f in listdir(f_yodie_anns) if isfile(join(f_yodie_anns, f))]
    for ann in ann_files:
        utils.multi_thread_large_file_tasking(join(f_yodie_anns, ann), 20, do_index_cris,
                                              args=[es, doc_to_patient, doc_dict, container],
                                              file_encoding='iso-8859-1')
        print 'file %s [%s] done' % (ann, len(container))
    print 'num done %s' % len(container)
    print 'done'


def do_index_patient(patient_id, es):
    es.query_entity_to_index(patient_id)


def index_cris_patients():
    f_patient_doc = './hepc_pos_doc_brcid.txt'
    lines = utils.read_text_file(f_patient_doc, encoding='utf-8-sig')
    patients = []
    for l in lines:
        arr = l.split('\t')
        if arr[0] not in patients:
            patients.append(arr[0])
    print 'total patients %s %s' % (len(patients), patients[0])
    es = EntityCentricES.get_instance('./pubmed_test/es_cris_setting.json')
    utils.multi_thread_tasking(patients, 10, do_index_patient, args=[es])
    print 'done'


def mimic_load_text(text_file):
    print 'reading %s' % text_file
    s = datetime.now()
    lines = utils.read_text_file(text_file)
    print 'read in %s seconds' % (datetime.now() - s).seconds


def test():
    es = EntityCentricES.get_instance('./pubmed_test/es_setting.json')
    anns = utils.load_json_data('./pubmed_test/test_anns.json')['annotations'][0]
    # # print get_ctx_concept_id()
    # # index_ctx_concept(ann, index='pubmed')
    es.index_entity_data(hashlib.md5('J Parkinsons Dis').hexdigest().upper(),
                         'PMC5302030', anns,
                         {"pmcid:": "PMC5302030",
                          "fulltext":
                              "J Parkinsons Dis\n Could MAO-B Inhibitor Withdrawal Rather than "
                              "Nilotinib Benefit Explain the Dopamine Metabolite "
                              "Increase in Parkinsonian Study Subjects?"
                          })

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # index_cris_cohort()
    index_cris_patients()
