import utils
from elasticsearch import Elasticsearch
import nltk
import itertools
import csv
import json
from nltk.tokenize import sent_tokenize
import codecs


_es_host = 'http://10.200.102.23:9200/'
_es_instance = None
_window_size = 100

vocabulary_size = 8000
unknown_token = "UNKNOWN_TOKEN"
sentence_start_token = "SENTENCE_START"
sentence_end_token = "SENTENCE_END"


def get_es_instance():
    global _es_instance
    if _es_instance is None:
        _es_instance = Elasticsearch([_es_host])
    return _es_instance


def encode_doc_anns(doc_id, anns, index_name, doc_type):
    doc = get_es_instance().get(index_name, doc_id, doc_type=doc_type)
    fulltext = doc['fulltext']
    for ann in anns:
        pass


def extract_text(file_path, anns, window=20):
    text = utils.read_text_file_as_string(file_path)
    results = []
    for ann in anns:
        prev_words = nltk.word_tokenize(text[0:ann['start']])[-window:]
        next_words = nltk.word_tokenize(text[ann['end']:])[:window]
        result = {'annId': ann['id'], 'prev': prev_words, 'next': next_words}
        results.append(result)
    return results


def encode_text(file_path, encoding='utf-8'):
    # Read the data and append SENTENCE_START and SENTENCE_END tokens
    print "Reading CSV file..."
    # with open(file_path, 'rU') as f:
    with codecs.open(file_path, encoding=encoding) as rf:
        # reader = csv.reader(f, skipinitialspace=False)
        # reader.next()
        # Split full comments into sentences
        sentences = itertools.chain([json.loads(l)[0]['prev'] + json.loads(l)[0]['next'] for l in rf.readlines()])
        # Append SENTENCE_START and SENTENCE_END
        # sentences = ["%s %s %s" % (sentence_start_token, x, sentence_end_token) for x in ann_contexes]

    # Tokenize the sentences into words
    tokenized_sentences = [nltk.word_tokenize(' '.join(sent).lower()) for sent in sentences]

    # Count the word frequencies
    word_freq = nltk.FreqDist(itertools.chain(*tokenized_sentences))
    print "Found %d unique words tokens." % len(word_freq.items())

    # Get the most common words and build index_to_word and word_to_index vectors
    vocab = word_freq.most_common(vocabulary_size-1)
    index_to_word = [x[0] for x in vocab]
    index_to_word.append(unknown_token)
    word_to_index = dict([(w, i) for i, w in enumerate(index_to_word)])

    print word_to_index
    print "Using vocabulary size %d." % vocabulary_size
    return word_to_index


def export_validated_anns(file_path, es_instance):
    doc2anns = {}
    lines = utils.read_text_file(file_path)
    for l in lines:
        arr = l.split('\t')
        ann_locs = arr[2].split('_')
        ann = {'d': ann_locs[0][1:],
               'start': ann_locs[1][1:],
               'end': ann_locs[2][1:],
               'yodie-type': arr[3],
               'gt-type': arr[4]}
        doc2anns[ann['d']] = [ann] if ann['d'] not in doc2anns else [ann] + doc2anns[ann['d']]
    return doc2anns


if __name__ == "__main__":
    # encode_doc_anns('250834', [], 'mimic', 'eprdoc')
    # results = extract_text('./resources/text_test.csv', [{'id': 'ann1', 'start': 48, 'end': 55}], window=50)
    # print json.dumps(results)
    # encode_text('./resources/ann_contexts.json')
    export_validated_anns('./resources/valided_anns.tsv', None)
