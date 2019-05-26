import spacy
import en_core_web_sm as enmodel
# import sqldbutils as dutil
import utils
from os.path import join, exists, isfile
from os import listdir
import sys
import sentence_pattern as sp
import joblib as jl
import json
import numpy as np
from sklearn import cluster
from sklearn.metrics.pairwise import euclidean_distances
import scipy


def load_mode(model_name):
    if model_name == 'en':
        return enmodel.load()
    return None


def doc_processing(nlp, doc_text, anns, doc_id):
    """
    pick up relevant sentences with labelled annotations and send them for sentence level processing
    :param nlp:
    :param doc_text:
    :param anns:
    :return:
    """
    doc = nlp(doc_text)
    pos = 0
    matched_sent_anns = []
    for sent in doc.sents:
        idx = 0
        m_anns = []
        for ann in anns:
            if ann['s'] >= sent.start_char and ann['e'] <= sent.end_char:
                m_anns.append(ann)
            idx += 1
        if len(m_anns) > 0:
            # matched_sent_anns.append({'sent': sent, 'sent_offset': pos, 'anns': m_anns})
            # create a separate instance for each annotation
            for mn in m_anns:
                matched_sent_anns.append({'sent': sent, 'sent_offset': pos, 'anns': [mn]})
            anns = [ann for ann in anns if ann not in m_anns]
        # print '%s-%s, %s: [%s]' % (s, e, doc_text.index(sent.text), sent.text)
    # print matched_sent_anns
    ptn_inst = []
    for s in matched_sent_anns:
        ptn = sp.POSSentencePatternInst(s['sent'], s['anns'])
        ptn.process()
        ptn_inst.append(ptn)
        ptn.doc_id = doc_id
        # print '>>%s: >>%s\n' % (ret['sent'], ret['pattern'])
    return ptn_inst


def generalise_sent_pos(s):
    """
    generalise sentence pattern by POS tags only
    :param s:
    :return:
    """
    rets = []
    for token in s['sent']:
        e = token.idx + len(token.text)
        is_matched = False
        for ann in s['anns']:
            if token.idx >= ann['s'] and e <= ann['e']:
                rets.append((token.text, token.pos_, True, ann['signed_label'], ann['gt_label']))
                is_matched = True
                break
        # print '%s-%s, %s: [%s]' % (token.idx, e, token.idx, token.text)
        if not is_matched:
            rets.append((token.text, token.pos_))
    return {"sent": s['sent'].text, 'pattern': rets}


def word2vec_testing(text1, text2):
    nlp = spacy.load('en_core_web_lg')
    doc1 = nlp(text1)
    doc2 = nlp(text2)
    print '%s vs %s \nsimilarity: %s' % (text1, text2, doc1.similarity(doc2))


def download_docs(doc_ids, query, db_conn_setting, out_put_folder):
    """
    download clinical notes from EHR
    :param doc_ids:
    :param query:
    :param db_conn_setting:
    :return:
    """
    db_cnn = dutil.get_db_connection_by_setting(db_conn_setting)
    results = []
    q = query.format(**{'ids': ','.join(['\'%s\'' % did for did in doc_ids])} )
    print 'querying [%s]' % q
    print q
    dutil.query_data(q, results, db_cnn)
    for r in results:
        if r['textcontent'] is not None:
            utils.save_string(r['textcontent'].decode('cp1252').replace(chr(13), ' '), join(out_put_folder, r['cn_doc_id'] + '.txt'), encoding='utf-8')


def do_process_labelled_doc(doc_anns, container):
    doc_id = doc_anns[0]
    anns = doc_anns[1]
    doc = utils.read_text_file_as_string(join(working_folder, 'docs', '%s.txt' % doc_id),
                                         encoding='utf-8')    # print doc
    container += doc_processing(nlp,
                                doc,
                                anns,
                                doc_id)


def process_labelled_docs(labelled_file, corpus_model_file, mini_comp_file):
    corpus_analyzer = None
    if not isfile(corpus_model_file):
        # load labelled data
        ann_lines = utils.read_text_file(labelled_file)
        prev_doc = None
        anns = []
        doc_anns = []
        ptn_insts = []
        for ls in ann_lines:
            l = ls.split('\t')
            doc_id = l[0]
            if prev_doc != doc_id:
                if prev_doc is not None:
                    if exists(join(working_folder, 'docs', '%s.txt' % prev_doc)):
                        doc_anns.append((prev_doc, anns))
                anns = []
                prev_doc = doc_id
            anns.append({'s': int(l[1]), 'e': int(l[2]), 'signed_label': l[3], 'gt_label': l[4]})
        if prev_doc is not None:
            if exists(join(working_folder, 'docs', '%s.txt' % prev_doc)):
                doc_anns.append((prev_doc, anns))
        # mutithreading do processing labelled docs
        print 'processing docs...'
        utils.multi_thread_tasking(doc_anns, 30, do_process_labelled_doc, args=[ptn_insts])
        print 'merging patterns..'
        corpus_analyzer = sp.CorpusAnalyzer()
        for pi in ptn_insts:
            corpus_analyzer.add_pattern(pi)
        corpus_analyzer.serialise(corpus_model_file)
    else:
        corpus_analyzer = sp.CorpusAnalyzer.load_seralisation(corpus_model_file)
        # corpus_analyzer.show()
        # pt_insts = corpus_analyzer.pattern_to_insts

    if isfile(mini_comp_file):
        corpus_analyzer.load_mini_comp_dict(mini_comp_file)
    else:
        corpus_analyzer.produce_save_comp_dict(mini_comp_file)
    corpus_analyzer.show_mini_comp_patterns()
    # generate_corpus_model(corpus_analyzer)
    return corpus_analyzer


def generate_corpus_model(corpus_analyzer):
    mini_results = do_mini_comp_analysis(corpus_analyzer)
    for mr in mini_results:
        mr['hard_ones'] = {}
        for hard in mr['hard']:
            # hard is like: ["her", 0.5714285714285714, 7]
            mr['hard_ones'][hard[0]] = \
                do_sentence_scope_analysis(corpus_analyzer, mr['mini_comp_pattern'], mr['variation_pos'], hard[0])
    print '*********corpus results********'
    print json.dumps(mini_results)


def do_sentence_scope_analysis(corpus_analyzer, mimi_comp, var_idxs, var_text):
    return corpus_analyzer.analyse_sentence_scope(mimi_comp, var_idxs, var_text)


def do_mini_comp_analysis(corpus_analyzer):
    # get frequent minimal components and analysing its variant forms
    # to figure out easy/difficult situations
    freq_mcs = corpus_analyzer.get_mini_comp_pattern_by_freq(freq=2)
    freq_mcs_results = []
    for mc in freq_mcs:
        if mc[0] is None:
            continue
        p_arr = mc[0].split('->')
        idxs = []
        for i in range(0, len(p_arr)):
            if p_arr[i] != 'KCL':
                idxs.append(i)
        freq_mcs_results.append(corpus_analyzer.analyse_mini_comp_pattern(mc[0], idxs))
    print 'frequent mini component results: \n%s' % json.dumps(freq_mcs_results)
    return freq_mcs_results


def test_gen_patterns(child_ptn, corpus_analyzer):
    correct_ptns = []
    incorrect_ptns = []
    for ptn in corpus_analyzer.pattern_to_insts:
        if ptn.find(child_ptn) >= 0:
            for inst in corpus_analyzer.pattern_to_insts[ptn]:
                if inst.annotations[0]['gt_label'] != '-':
                    correct_ptns.append(inst.sentence) \
                        if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label'] \
                        else incorrect_ptns.append(inst.sentence)
    print '----------'
    print '%s total %s, confidence %s' % \
          (child_ptn, len(correct_ptns) + len(incorrect_ptns),
           len(correct_ptns) / (1.0 * (len(correct_ptns) + len(incorrect_ptns))))
    print '**correct: %s' % correct_ptns
    print '**incorrect: %s' % incorrect_ptns
    print '----------\n\n'


def nlp_process_doc(doc_file, container):
    container.append(nlp(utils.read_text_file_as_string(doc_file)))


def test_serialisation(nlp, docs_path, models_path):
    docs = []
    utils.multi_thread_process_files(docs_path, 'txt', 10, nlp_process_doc, args=[docs])
    jl.dump(docs, models_path)


def test_load(models_path):
    docs = jl.load(models_path)
    if len(docs) > 0:
        doc = docs[0]
        for sent in doc.sents:
            print sent.text
            for token in sent:
                print (token.text, token.pos_)


def word2vect_clustering(nlp, docs, metric='euclidean',
                         cluster_prefix='cls', eps=3.0, min_samples=2):
    """
    word2doc dbscan clustering to merge short texts, e.g., concrete verb phrases
    :param nlp:
    :param docs:
    :param cluster_prefix:
    :param eps:
    :param min_samples:
    :return:
    """
    X = None
    for d in docs:
        if X is None:
            X = np.array([nlp(unicode(d)).vector.tolist()])
        else:
            X = np.concatenate((X, np.array([nlp(d).vector.tolist()])), axis=0)

    model = cluster.DBSCAN(eps=eps, min_samples=min_samples, metric=metric)
    labels = model.fit_predict(X)
    print labels
    cls2docs = {}
    for idx in xrange(len(labels)):
        if labels[idx] == -1:
            cls2docs[docs[idx]] = [docs[idx]]
        else:
            cls = cluster_prefix + str(labels[idx])
            arr = []
            if cls in cls2docs:
                arr = cls2docs[cls]
            else:
                cls2docs[cls] = arr
            arr.append(docs[idx])
    print cls2docs
    return cls2docs


def dbscan_predict(dbscan_model, X_new, metric=euclidean_distances):
    """
    dbscan model for assigning new data items to learnt clusters
    """
    # Result is noise by default
    y_new = np.ones(shape=len(X_new), dtype=int)*-1
    print dbscan_model.components_.shape

    # Iterate all input samples for a label
    for j, x_new in enumerate(X_new):
        # Find a core sample closer than EPS
        for i, x_core in enumerate(dbscan_model.components_):
            s = metric(x_new.reshape(1, -1), x_core.reshape(1, -1))
            if s < dbscan_model.eps:
                print s, dbscan_model.eps
                # Assign label of x_core to x_new
                y_new[j] = dbscan_model.labels_[dbscan_model.core_sample_indices_[i]]
                break
    return y_new


def predict_exp(corpus_trans_file, ann_file, cache_file, output_file):
    # initialise pattern instances from documents
    if not isfile(cache_file):
        # load labelled data
        ann_lines = utils.read_text_file(ann_file)
        prev_doc = None
        anns = []
        doc_anns = []
        ptn_insts = []
        doc_to_pt = {}
        for ls in ann_lines:
            l = ls.split('\t')
            doc_id = l[1]
            doc_to_pt[doc_id] = l[0]
            if prev_doc != doc_id:
                if prev_doc is not None:
                    if exists(join(working_folder, 'docs', '%s.txt' % prev_doc)):
                        doc_anns.append((prev_doc, anns))
                anns = []
                prev_doc = doc_id
            anns.append({'s': int(l[2]), 'e': int(l[3]), 'signed_label': l[4], 'gt_label': l[5]})
        if prev_doc is not None:
            if exists(join(working_folder, 'docs', '%s.txt' % prev_doc)):
                doc_anns.append((prev_doc, anns))
        # mutithreading do processing labelled docs
        print 'processing docs...'
        utils.multi_thread_tasking(doc_anns, 30, do_process_labelled_doc, args=[ptn_insts])
        jl.dump({'insts': ptn_insts, 'doc_to_pt': doc_to_pt}, cache_file)
    else:
        cached = jl.load(cache_file)
        ptn_insts = cached['insts']
        doc_to_pt = cached['doc_to_pt']

    cp = sp.CorpusPredictor.load_corpus_model(corpus_trans_file)
    ret = []
    for inst in ptn_insts:
        print 'predicting [%s]...' % inst.sentence
        acc = cp.predcit(inst)
        print 'accuracy: %s' % acc
        ann = inst.annotations[0]
        ret.append((doc_to_pt[inst.doc_id], inst.doc_id, str(ann['s']), str(ann['e']),
                    ann['signed_label'], ann['gt_label'], str(acc)))
    s = []
    for r in ret:
        s.append(u'\t'.join(r))
    print u'\n'.join(s)
    utils.save_json_array(ret, output_file)
    return ret


def produce_weka_output(predict_output_file, orig_features_file,
                        merged_output_file, arrf_file,
                        threshold=.70, mode='threshold'):
    orig_data_lines = utils.read_text_file(orig_features_file)
    ret = utils.load_json_data(predict_output_file)
    ptn2anns = {}
    for r in ret:
        ptn = r[0]
        if ptn not in ptn2anns:
            ptn2anns[ptn] = {'posM':0, 'negM':0, 'hisM':0, 'otherM':0}
        if mode == 'threshold':
            if float(r[6]) >= threshold:
                ptn2anns[ptn][r[4]] += 1
        elif mode == 'weighted_sum':
            ptn2anns[ptn][r[4]] += float(r[6])

    rows = []
    arrf_header = """@RELATION	hepc

@ATTRIBUTE	Total_Mentions	NUMERIC
@ATTRIBUTE	Positive_Mentions	NUMERIC
@ATTRIBUTE	History_hypothetical_Mentions	NUMERIC
@ATTRIBUTE	Negative_Mentions	NUMERIC
@ATTRIBUTE	Other_Experiencers	NUMERIC
@ATTRIBUTE	AT_Total_Mentions	NUMERIC
@ATTRIBUTE	AT_Positive_Mentions	NUMERIC
@ATTRIBUTE	AT_History_hypothetical_Mentions	NUMERIC
@ATTRIBUTE	AT_Negative_Mentions	NUMERIC
@ATTRIBUTE	AT_Other_Experiencers	NUMERIC
@ATTRIBUTE	class	{positive,negative,unknown}


@DATA
"""
    arrf_rows = []
    for l in orig_data_lines:
        arr = l.split('\t')
        ptn = arr[0]
        new_line = arr[:6] + \
                   ([str(ptn2anns[ptn]['posM'] + ptn2anns[ptn]['negM'] + ptn2anns[ptn]['hisM'] + ptn2anns[ptn]['otherM']),
                                str(ptn2anns[ptn]['posM']),
                                str(ptn2anns[ptn]['hisM']),
                                str(ptn2anns[ptn]['negM']),
                                str(ptn2anns[ptn]['otherM'])] if ptn in ptn2anns else ['0','0','0','0','0']) + \
                   [arr[6]]
        rows.append(new_line)
        arrf_rows.append(','.join(new_line[1:]))

    utils.save_string(arrf_header + '\n'.join(arrf_rows), arrf_file)
    utils.save_string('\n'.join(['\t'.join(r) for r in rows]), merged_output_file)


if __name__ == "__main__":
    # word2vec_testing(u'for Tested', u'for Diagnosed')
    word2vect_clustering(
        spacy.load('en'),
        [
            u'for is',
            u'from suffers',
            u'for was',
            u'for start',
            u'with infected',
            u'about spoken',
            u'about informed',
            u'about talking',
            u'for Tested',
            u'for treated',
            u'for diagnosed',
            u'for Diagnosed',
        ], eps=9)
    # sentence_parsing(u'Routine blood investigation and virology for Hep C done')
    # reload(sys)
    # sys.setdefaultencoding('cp1252')
    # working_folder = "/Users/honghan.wu/Documents/working/KCL/publications/actionable_trans"
    # # 1. download texts from EHR
    # # download_docs([],
    # #               'select cn_doc_id, textcontent from SQLCRIS_User.Kconnect.working_docs where cn_doc_id in ({ids})',
    # #               join(working_folder, "dbcnn.json"),
    # #               join(working_folder, 'docs') )
    # # 2. process doc
    # # nlp = load_mode('en')
    # # corpus_analyzer = process_labelled_docs(join(working_folder, 'labelled.txt'),
    # #                                         join(working_folder, 'cris_hepc_model_test.pickle'),
    # #                                         join(working_folder, 'cris_hepc_mini_comp_dict.pickle'))
    # # predict annotation accuracy
    # # predict_exp(join(working_folder, 'cris_corpus_trans.json'),
    # #             join(working_folder, 'validate_unknown_200hepc.txt'),
    # #             join(working_folder, 'validation_cache.pickle'),
    # #             join(working_folder, 'predicted_200hepc_output.json'))
    # produce_weka_output(join(working_folder, 'predicted_200hepc_output.json'),
    #                     join(working_folder, 'orig_gt_labelled.txt'),
    #                     join(working_folder, 'merged_hepc_features.json'),
    #                     join(working_folder, 'merged_hepc.9.arff'), threshold=.9)
