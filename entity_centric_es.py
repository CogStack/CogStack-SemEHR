from elasticsearch import Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions
import hashlib
import utils
import json
from os.path import join


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
        self._es_instance = Elasticsearch([es_host], serializer=JSONSerializerPython2())
        self._index = 'semehr'
        self._concept_doc_type = 'ctx_concept'
        self._entity_doc_type = 'user'

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

    def init_index(self, mapping):
        if self._es_instance.indices.exists(self.index_name):
            self._es_instance.indices.delete(self.index_name)
        self._es_instance.indices.create(self.index_name)
        for t in mapping:
            self._es_instance.indices.put_mapping(doc_type=t, body=mapping[t])

    def index_ctx_concept(self, ann):
        data = {
            "doc": {
                "cui": ann['features']['inst'],
                "negation": ann['features']['Negation'],
                "experiencer": ann['features']['Experiencer'],
                "temporality": ann['features']['Temporality'],
                "prefLabel": ann['features']['PREF'],
                "vocabularies": ann['features']['VOCABS'],
                "STY": ann['features']['STY']
            },
            "doc_as_upsert": True
        }
        ctx_id = EntityCentricES.get_ctx_concept_id(ann)
        print json.dumps(data)
        self._es_instance.update(index=self.index_name, doc_type=self.concept_doc_type, id=ctx_id, body=data)

    def index_entity_data(self, entity_id, doc_id, anns=None, article=None):
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
                                "pmcid": doc_id,
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

        if article is not None:
            scripts.append("if (ctx._source.articles == null) " \
                           "{ ctx._source.articles = [article] } else " \
                           "{ ctx._source.articles = ctx._source.articles + article}")
            data['params']['article'] = article
            data['upsert']['articles'] = [article]

        data['script'] = ';'.join(scripts)

        print json.dumps(data)
        print entity_id
        self._es_instance.update(index=self.index_name, doc_type=self.entity_doc_type, id=entity_id, body=data)

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
                              "fulltext": utils.read_text_file(join(full_text_path, pmcid))
                              })


def index_pubmed():
    es = EntityCentricES.get_instance('./pubmed_test/es_setting.json')
    doc_details = utils.load_json_data('./pubmed_test/pmc_docs.json')
    pmcid_to_journal = {}
    for d in doc_details:
        if 'pmcid' in d and 'journalTitle' in d:
            pmcid_to_journal[d['pmcid']] = d['journalTitle']
    # load anns
    utils.multi_thread_large_file_tasking('./pubmed_test/test_anns.json', 10, do_index_pubmed,
                                          args=[es, pmcid_to_journal, './pubmed_test/fulltext'])
    print 'done'


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
    index_pubmed()