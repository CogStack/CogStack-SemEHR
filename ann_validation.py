import utils
from elasticsearch import Elasticsearch
import nltk
import itertools
import csv
import json
from nltk.tokenize import sent_tokenize
import codecs


_es_host = 'http://localhost:9200/'
_es_instance = None
_es_index_name = 'hepcpos_300'
_es_doc_type = 'eprdoc'
_window_size = 100

vocabulary_size = 8000
unknown_token = "UNKNOWN_TOKEN"
sentence_start_token = "SENTENCE_START"
sentence_end_token = "SENTENCE_END"
ann_type_tokens = ['posM', 'negM', 'hisM', 'otherM']


def get_es_instance():
    global _es_instance
    if _es_instance is None:
        _es_instance = Elasticsearch([_es_host])
    return _es_instance


def encode_doc_anns(d2anns, ann_ctx_file=None):
    ann_context_list = []
    for d in d2anns:
        print('getting %s' % d)
        doc = get_es_instance().get(d2anns[d][0]['index'], d, doc_type=_es_doc_type)
        ann_context_list += extract_text(doc['_source']['fulltext'], d2anns[d])
    if ann_ctx_file is not None:
        utils.save_json_array(ann_context_list, ann_ctx_file)
        print('annotation context results saved to %s' % ann_ctx_file)
    return ann_context_list


def extract_text(text, anns, window=20):
    results = []
    for ann in anns:
        prev_words = nltk.word_tokenize(text[0:ann['start']])[-window:]
        next_words = nltk.word_tokenize(text[ann['end']:])[:window]
        result = {'annId': ann['id'], 'prev': prev_words, 'next': next_words, 'label': ann['gt-type']}
        results.append(result)
    return results


def read_ann_contexts(file_path, encoding='utf-8'):
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


def encode_text(ann_ctxs, word_to_index_file=None):
    # Tokenize the sentences into words
    tokenized_sentences = [nltk.word_tokenize(' '.join(ctx['prev'] + ctx['next']).lower()) for ctx in ann_ctxs]

    # Count the word frequencies
    word_freq = nltk.FreqDist(itertools.chain(*tokenized_sentences))
    print "Found %d unique words tokens." % len(word_freq.items())

    # Get the most common words and build index_to_word and word_to_index vectors
    vocab = word_freq.most_common(vocabulary_size - len(ann_type_tokens))
    index_to_word = [x[0] for x in vocab]
    index_to_word += ann_type_tokens
    word_to_index = dict([(w, i) for i, w in enumerate(index_to_word)])
    if word_to_index_file is not None:
        utils.save_json_array(word_to_index, word_to_index_file)
    return word_to_index


def export_validated_anns(file_path, es_instance):
    doc2anns = {}
    lines = utils.read_text_file(file_path)
    for l in lines:
        arr = l.split('\t')
        if arr[4] == '-':
            print('%s not validated' % arr[2])
            continue
        ann_locs = arr[2].split('_')
        ann = {'d': ann_locs[0][1:],
               'id': arr[2],
               'start': int(ann_locs[1][1:]),
               'end': int(ann_locs[2][1:]),
               'yodie-type': arr[3],
               'gt-type': arr[4],
               'index': arr[5] if len(arr) == 6 else 'hepcpos_300'}
        doc2anns[ann['d']] = [ann] if ann['d'] not in doc2anns else [ann] + doc2anns[ann['d']]
    return doc2anns


def encode_ann_ctx(dic_file, ann_ctx_file, output_file=None):
    word_to_index = utils.load_json_data(dic_file)
    ann_ctxs = utils.load_json_data(ann_ctx_file)
    encoded = []
    for ann in ann_ctxs:
        encoded.append({'prev': [word_to_index[w.lower()] for w in nltk.word_tokenize(' '.join(ann['prev']).lower())],
                        'next': [word_to_index[w.lower()] for w in nltk.word_tokenize(' '.join(ann['next']).lower())],
                        'label': ann['label'],
                        'annId': ann['annId'],
                        'label_encoded': word_to_index[ann['label']]})
    if output_file is not None:
        utils.save_json_array(encoded, output_file)
    return encoded


def preprocess_validated_anns(validated_ann_file, output_ctx_file, output_dic_file):
    d2anns = export_validated_anns(validated_ann_file, None)
    print('annotations read - #docs: %s' % len(d2anns))
    ann_ctxs = encode_doc_anns(d2anns, output_ctx_file)
    encode_text(ann_ctxs, output_dic_file)

if __name__ == "__main__":
    # encode_doc_anns('250834', [], 'mimic', 'eprdoc')
    # results = extract_text('./resources/text_test.csv', [{'id': 'ann1', 'start': 48, 'end': 55}], window=50)
    # print json.dumps(results)
    # encode_text('./resources/ann_contexts.json')
    # d2anns = export_validated_anns('./resources/validated_anns.tsv', None)
    # print('annotations read - #docs: %s' % len(d2anns))
    # ann_ctxs = encode_doc_anns(d2anns, './resources/ann_ctx.json')
    # encode_text(ann_ctxs, './resources/word_to_index.json')
    # encode_ann_ctx('./resources/word_to_index.json', './resources/ann_ctx.json', './resources/encoded_ann_ctx.json')
    # preprocess_validated_anns('./resources/validated_anns_full.tsv',
    #                           './resources/ann_ctx_full.json',
    #                           './resources/word_to_index_full.json')
    encode_ann_ctx('./resources/word_to_index_full.json', './resources/ann_ctx_full.json',
                   './resources/encoded_ann_ctx_full.json')
