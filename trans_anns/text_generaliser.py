import spacy
import en_core_web_sm as enmodel
import sqldbutils as dutil
import utils
from os.path import join, exists, isfile
from os import listdir
import sys
import sentence_pattern as sp
import joblib as jl
import json


def load_mode(model_name):
    if model_name == 'en':
        return enmodel.load()
    return None


def doc_processing(nlp, doc_text, anns):
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
            matched_sent_anns.append({'sent': sent, 'sent_offset': pos, 'anns': m_anns})
            anns = [ann for ann in anns if ann not in m_anns]
        # print '%s-%s, %s: [%s]' % (s, e, doc_text.index(sent.text), sent.text)
    # print matched_sent_anns
    ptn_inst = []
    for s in matched_sent_anns:
        ptn = sp.POSSentencePatternInst(s['sent'], s['anns'])
        ptn.process()
        ptn_inst.append(ptn)
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
                                anns)


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
    # corpus_analyzer.show_mini_comp_patterns()
    # do_mini_comp_analysis(corpus_analyzer)
    do_sentence_scope_analysis(corpus_analyzer, 'ADJ->KCL->NOUN', [0,2], 'his =#= status')


def do_sentence_scope_analysis(corpus_analyzer, mimi_comp, var_idxs, var_text):
    corpus_analyzer.analyse_sentence_scope(mimi_comp, var_idxs, var_text)


def do_mini_comp_analysis(corpus_analyzer):
    # get frequent minimal components and analysing its variant forms
    # to figure out easy/difficult situations
    freq_mcs = corpus_analyzer.get_mini_comp_pattern_by_freq(freq=3)
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


if __name__ == "__main__":
    # word2vec_testing(u'therapy', u'treatment')
    # sentence_parsing(u'Routine blood investigation and virology for Hep C done')
    reload(sys)
    sys.setdefaultencoding('cp1252')
    working_folder = ""
    # 1. download texts from EHR
    # download_docs([],
    #               'select cn_doc_id, textcontent from SQLCRIS_User.Kconnect.working_docs where cn_doc_id in ({ids})',
    #               join(working_folder, "dbcnn.json"),
    #               join(working_folder, 'docs') )
    # 2. process doc
    nlp = load_mode('en')
    process_labelled_docs(join(working_folder, 'labelled.txt'),
                          join(working_folder, 'cris_hepc_model_test.pickle'),
                          join(working_folder, 'cris_hepc_mini_comp_dict.pickle'))
    # model_file = ''
    # text_files_path = ''
    # test_serialisation(nlp,
    #                    text_files_path,
    #                    model_file
    #                    )
    # test_load(model_file)
