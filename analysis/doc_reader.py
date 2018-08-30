import spacy, utils
from semquery import SemEHRES
import re
from os import listdir
from os.path import isdir, isfile, join


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
                yield [w for w in tokens if w not in StreamedDocs.get_customised_stopwords()]

    @staticmethod
    def get_spacy_en_stopwords():
        return FileIterDocs.get_nlp().Defaults.stop_words

    @staticmethod
    def get_customised_stopwords():
        return ['', 'the', 'a', 'an', 'these', 'that']

    @staticmethod
    def simple_tokenize(w):
        return re.sub(r'([^a-zA-Z0-9]*)$|(^[^a-zA-Z0-9]*)', '', w)

    @staticmethod
    def get_nlp():
        if StreamedDocs._spacy_instance is None:
            print 'loading nlp instance...'
            StreamedDocs._spacy_instance = spacy.load('en_core_web_sm')
        return StreamedDocs._spacy_instance


class FileIterDocs(StreamedDocs):
    """
    file based document iterator
    SemEHR annotation to be marked before embedding learning
    """

    def __init__(self, doc_ids, doc_path_template, doc_to_anns):
        self.docs = doc_ids
        self._path_template = doc_path_template
        self._anns = None
        self._need_preprocess = False
        self._doc_to_anns = doc_to_anns

    def add_annotations(self, anns):
        self._anns = anns

    @property
    def need_preprocess(self):
        return self._need_preprocess

    @need_preprocess.setter
    def need_preprocess(self, value):
        self._need_preprocess = value

    @staticmethod
    def preprocess(text):
        return re.sub(r'\s{2,}', '\n', text)

    @staticmethod
    def match_ann_in_text(t, a, defaultIndex):
        mit = re.finditer(r"([\s\.;\,$\?!:/\('\"]|^)" + a + "([\s\.;\,\?!:/\)'\"]|$)", t, re.IGNORECASE)
        poz = []
        for m in mit:
            if m is not None:
                p = m.end(1)
                poz.append([abs(p - defaultIndex), p])
        poz = sorted(poz, key=lambda x: x[0])
        if len(poz) > 0:
            return poz[0][1]
        else:
            return -1

    def markup_annotations(self, text):
        # sort anns
        anns = self._anns
        anns = sorted(anns, key=lambda x: x.offset_start)

        inserts = []
        for ann in anns:
            start = self.match_ann_in_text(text, ann.string_orig, int(ann.offset_start))
            end = start + len(ann.string_orig)
            if start < 0:
                start = int(ann.offset_start)
                end = ann.offset_end
            inserts.append({'p': start, 'insert': ' ' + ann.annotator_label + '-' + ann.concept + ' '})
        inserts = sorted(inserts, key=lambda x: x['p'])

        prev_pos = 0
        ret = ''
        print inserts
        for insert in inserts:
            ret += text[prev_pos:insert['p']] + insert['insert']
            prev_pos = insert['p']
        ret += text[prev_pos:]
        return ret

    def ann_to_labelled_data(self, text, window=3):
        # sort anns
        anns = self._anns
        anns = sorted(anns, key=lambda x: x.offset_start)

        data_x = []
        data_y = []
        for ann in anns:
            start = self.match_ann_in_text(text, ann.string_orig, ann.offset_start)
            end = start + len(ann.string_orig)
            if start < 0:
                start = ann.offset_start
                end = ann.offset_end
            x = []
            x.append(text[:start].split()[-window])
            x.append(text[end:].split())[:window]
            data_x.append(x)
            data_y.append(ann.annotator_label + '-' + ann.concept)
        return data_x, data_y

    def get_doc_by_id(self, doc_id):
        doc_path = self._path_template.format(**{'doc_id': doc_id})
        print 'working on %s' % doc_path
        text = utils.read_text_file_as_string(doc_path)
        if doc_id in self._doc_to_anns:
            self.add_annotations(self._doc_to_anns[doc_id])
        if self.need_preprocess:
            text = FileIterDocs.preprocess(text)
        return self.markup_annotations(text)


class SemEHRAnn(object):
    """
    semantic annotation object
    """
    def __init__(self):
        self._offset_start = -1
        self._offset_end = -1
        self._string_orig = None
        self._concept = None
        self._annotator_label = None

    @property
    def offset_start(self):
        return self._offset_start

    @offset_start.setter
    def offset_start(self, value):
        self._offset_start = value

    @property
    def offset_end(self):
        return self._offset_end

    @offset_end.setter
    def offset_end(self, value):
        self._offset_end = value

    @property
    def string_orig(self):
        return self._string_orig

    @string_orig.setter
    def string_orig(self, value):
        self._string_orig = value

    @property
    def annotator_label(self):
        return self._annotator_label

    @annotator_label.setter
    def annotator_label(self, value):
        self._annotator_label = value

    @property
    def concept(self):
        return self._concept

    @concept.setter
    def concept(self, value):
        self._concept = value


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


def load_tsv_anns(tsv_file):
    doc_to_anns = {}
    for l in utils.read_text_file(tsv_file):
        arrs = l.split('\t')
        doc = arrs[0]
        anns = [] if doc not in doc_to_anns else doc_to_anns[doc]
        ann = SemEHRAnn()
        ann.offset_start = arrs[1]
        ann.offset_end = arrs[2]
        ann.string_orig = arrs[3]
        ann.concept = arrs[4]
        ann.annotator_label = arrs[5]
        anns.append(ann)
        if doc not in doc_to_anns:
            doc_to_anns[doc] = anns
    return doc_to_anns


def process_batched_docs(folder_path, out_folder):
    if isdir(folder_path):
        for f in listdir(folder_path):
            if isfile(join(folder_path, f)):
                t = utils.read_text_file_as_string(join(folder_path, f))
                print 'processing %s' % join(folder_path, f)
                print t
                mit = re.finditer(r'^(\d+)\,\"', t, re.MULTILINE)
                prev_pos = 0
                prev_id = None
                for m in mit:
                    if prev_pos > 0:
                        utils.save_string(t[prev_pos:m.start()-2], join(out_folder, prev_id))
                    prev_pos = m.end()
                    prev_id = m.string[m.start(1):m.end(1)]
                if prev_id is not None:
                    utils.save_string(t[prev_pos:len(t) - 1], join(out_folder, prev_id))
                else:
                    print 'ERROR!! pattern not found in %s' % join(folder_path, f)


def test():
    fds = FileIterDocs(['34021'], '/Users/honghan.wu/Documents/UoE/working_folder/sample_docs/doc_{doc_id}.txt')
    fds.need_preprocess = True
    anns = []
    ann = SemEHRAnn()
    ann.offset_start = 47
    ann.offset_end = 56
    ann.string_orig = 'discharge'
    ann.concept = 'C123124'
    ann.annotator_label = 'posM'
    anns.append(ann)
    fds.add_annotations(anns)
    for s in fds:
        print s


if __name__ == "__main__":
    docs_path = 'C:/Users/HWu/Desktop/validated_docs/docs'
    d2anns = load_tsv_anns('C:/Users/HWu/Desktop/validated_docs/anns/anns_dumped.csv')
    docs = [f for f in listdir(docs_path)]
    fdocs = FileIterDocs(docs, docs_path + '/{doc_id}', d2anns)
    c = 0
    for d in fdocs:
        print d
        c += 1
        if c > 200:
            break
