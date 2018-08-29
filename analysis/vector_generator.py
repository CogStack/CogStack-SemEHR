import gensim, logging, spacy, os
import utils
from doc_reader import FileIterDocs, QueryResultDocs


def train_word2vec(doc_ids, doc_path_template, model_file):
    docs = FileIterDocs(doc_ids, doc_path_template)
    model = gensim.models.Word2Vec(docs)
    model.save(model_file)
    print 'model trained & saved'
    return model


def train_word2vec_from_ES(es_config, query, model_file):
    """
    query docs from elasticsearch to train word2vec
    :param es_config:
    :param query:
    :param model_file:
    :return:
    """
    q_docs = QueryResultDocs(es_config, query)
    model = gensim.models.Word2Vec(q_docs, workers=40)
    model.save(model_file)
    print 'model trained & saved'
    return model


def test_model(model_file):
    model = gensim.models.Word2Vec.load(model_file)
    print model.most_similar(positive=['stroke'])
    print model.similarity('man', 'patient')


if __name__ == "__main__":
    docs = ['605171', '34021', '38734', '726955', '1021104', '1124892']
    d_path_template = '/Users/honghan.wu/Documents/UoE/working_folder/sample_docs/doc_{doc_id}.txt'
    w2v_model_file = '/Users/honghan.wu/Documents/UoE/working_folder/models/word2vec_6docs'

    ischemic_stroke_model = '/Users/honghan.wu/Documents/UoE/working_folder/models/word2vec_ischemic_stroke_1065'
    es_config = './conf/mimic_es_setting.json'
    query = 'C3272363' # Ischemic Stroke
    train_word2vec_from_ES(es_config, query, ischemic_stroke_model)
    # train_word2vec(docs, d_path_template, w2v_model_file)
    test_model(ischemic_stroke_model)
