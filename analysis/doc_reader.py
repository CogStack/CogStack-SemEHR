import spacy, utils
from semquery import SemEHRES
import re


class StreamedDocs(object):
    """
    effectively an abstract class for doing streaming docs
    """
    _spacy_instance = None

    def get_doc_by_id(self, doc_id):
        pass

    def __iter__(self):
        nlp = FileIterDocs.get_nlp()
        for d in self.docs:
            doc = nlp(self.get_doc_by_id(d))
            for line in doc.sents:
                tokens = [StreamedDocs.simple_tokenize(w) for w in line.text.split()]
                yield [w for w in tokens if w not in nlp.Defaults.stop_words and w not in ['']]

    @staticmethod
    def simple_tokenize(w):
        return re.sub(r'([^a-zA-Z]*)$|(^[^a-zA-Z]*)', '', w)

    @staticmethod
    def get_nlp():
        if StreamedDocs._spacy_instance is None:
            print 'loading nlp instance...'
            StreamedDocs._spacy_instance = spacy.load('en')
        return StreamedDocs._spacy_instance


class FileIterDocs(StreamedDocs):
    """
    file based document iterator
    SemEHR annotation to be marked before embedding learning
    """

    def __init__(self, doc_ids, doc_path_template):
        self._doc_ids = doc_ids
        self._path_template = doc_path_template

    def get_doc_by_id(self, doc_id):
        doc_path = self._path_template.format(**{'doc_id': doc_id})
        return utils.read_text_file_as_string(doc_path)


class QueryResultDocs(StreamedDocs):
    """
    ES Query based streaming docs
    - wraps ESDocReader to query and return fulltext of docs
    """
    def __init__(self, es_config, query):
        self.reader = ESDocReader(es_config)
        self.docs = self.reader.query_doc_ids(query)
        print '#docs: %s' % (len(self.docs))

    def get_doc_by_id(self, doc_id):
        return self.reader.get_doc_fulltext(doc_id)


class ESDocReader(object):
    """
    elasticsearch document reader
    - two steps to read ES docs:
    1) query to get ids;
    2) query doc fulltext for given doc id;
    """
    def __init__(self, es_config):
        es = utils.load_json_data(es_config)
        self.semquery = SemEHRES(es['_es_host'], es['_es_index'],
                                 es['_doc_type'], es['_concept_type'],
                                 es['_patient_type'])
        self.es = es

    def query_doc_ids(self, query):
        return self.semquery.search_by_scroll(query, self.es['_doc_type'], include_fields=['_id'])

    def get_doc_fulltext(self, doc_id):
        print 'reading %s' % doc_id
        d = self.semquery.get_doc_detail(doc_id, doc_type=self.es['_doc_type'])
        if d is not None:
            return d['fulltext']


if __name__ == "__main__":
    pass