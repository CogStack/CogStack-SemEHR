import utils
import sqldbutils as dutil
import json
from os.path import join
import ontotextapi as oi
import random
# from study_analyzer import StudyAnalyzer
from ann_post_rules import AnnRuleExecutor
import trans_anns.sentence_pattern as tssp
import trans_anns.text_generaliser as tstg


db_conn_type = 'mysql'
my_host = ''
my_user = ''
my_pwd = ''
my_db = ''
my_sock = ''


def get_doc_detail_by_id(doc_id, fulltext_date_by_doc_id):
    sql = fulltext_date_by_doc_id.format(**{'doc_id': doc_id})
    docs = []
    dutil.query_data(sql, docs)
    return docs


def load_all_docs():
    sql = "select TextContent, Date, src_table, src_col, BrcId, CN_Doc_ID from sqlcris_user.KConnect.vw_hepcpos_docs"
    docs = []
    dutil.query_data(sql, docs)
    return docs


def do_save_file(doc, folder):
    utils.save_string(unicode(doc['TextContent'], errors='ignore'), join(folder, doc['CN_Doc_ID'] + '.txt'))
    doc['TextContent'] = ''
    doc['Date'] = long(doc['Date'])
    utils.save_json_array(doc, join(folder, doc['CN_Doc_ID'] + '.json'))


def dump_doc_as_files(folder):
    utils.multi_thread_tasking(load_all_docs(), 10, do_save_file, args=[folder])


def populate_patient_concept_table(cohort_name, concepts, out_file,
                                   patients_sql, concept_doc_freq_sql):
    patients = []
    dutil.query_data(patients_sql.format(cohort_name), patients)
    id2p = {}
    for p in patients:
        id2p[p['brcid']] = p

    non_empty_concepts = []
    for c in concepts:
        patient_concept_freq = []
        print 'querying %s...' % c
        dutil.query_data(concept_doc_freq_sql.format(c, cohort_name), patient_concept_freq)
        if len(patient_concept_freq) > 0:
            non_empty_concepts.append(c)
            for pc in patient_concept_freq:
                id2p[pc['brcid']][c] = str(pc['num'])

    label2cid = {}
    concept_labels = []
    for c in non_empty_concepts:
        label = oi.get_concept_label(c)
        label2cid[label] = c
        concept_labels.append(label)
    concept_labels = sorted(concept_labels)
    s = '\t'.join(['brcid'] + concept_labels) + '\n'
    for p in patients:
        s += '\t'.join([p['brcid']] + [p[label2cid[k]] if label2cid[k] in p else '0' for k in concept_labels]) + '\n'
    utils.save_string(s, out_file)
    print 'done'


def generate_skip_term_constrain(study_analyzer,
                                 skip_term_sql):
    if len(study_analyzer.skip_terms) > 0:
        return skip_term_sql.format(', '.join(['\'%s\'' % t for t in study_analyzer.skip_terms]))
    else:
        return ''


def populate_patient_study_table(cohort_name, study_analyzer, out_file,
                                 patients_sql, term_doc_freq_sql):
    """
    populate result table for a given study analyzer instance
    :param cohort_name:
    :param study_analyzer:
    :param out_file:
    :return:
    """
    patients = []
    dutil.query_data(patients_sql.format(cohort_name), patients)
    id2p = {}
    for p in patients:
        id2p[p['brcid']] = p

    non_empty_concepts = []
    study_concepts = study_analyzer.study_concepts
    for sc in study_concepts:
        sc_key = '%s(%s)' % (sc.name, len(sc.concept_closure))
        concept_list = ', '.join(['\'%s\'' % c for c in sc.concept_closure])
        patient_term_freq = []
        if len(sc.concept_closure) > 0:
            data_sql = term_doc_freq_sql.format(**{'concepts': concept_list,
                                                   'cohort_id': cohort_name,
                                                   'extra_constrains':
                                                       ' \n '.join(
                                                           [generate_skip_term_constrain(study_analyzer)]
                                                           + [] if (study_analyzer.study_options is None or
                                                                    study_analyzer.study_options['extra_constrains'] is None)
                                                           else study_analyzer.study_options['extra_constrains'])})
            print data_sql
            dutil.query_data(data_sql, patient_term_freq)
        if len(patient_term_freq) > 0:
            non_empty_concepts.append(sc_key)
            for pc in patient_term_freq:
                id2p[pc['brcid']][sc_key] = str(pc['num'])

    concept_labels = sorted(non_empty_concepts)
    s = '\t'.join(['brcid'] + concept_labels) + '\n'
    for p in patients:
        s += '\t'.join([p['brcid']] + [p[k] if k in p else '0' for k in concept_labels]) + '\n'
    utils.save_string(s, out_file)
    print 'done'


def populate_patient_study_table_post_ruled(cohort_name, study_analyzer, out_file, rule_executor,
                                            sample_size, sample_out_file, ruled_ann_out_file,
                                            patients_sql, term_doc_anns_sql, skip_term_sql,
                                            db_conn_file):
    """
    populate patient study result with post processing to remove unwanted mentions
    :param cohort_name:
    :param study_analyzer:
    :param out_file:
    :param rule_executor:
    :param sample_size:
    :param sample_out_file:
    :return:
    """
    patients = []
    dutil.query_data(patients_sql.format(cohort_name), patients, dbconn=dutil.get_db_connection_by_setting(db_conn_file))
    id2p = {}
    for p in patients:
        id2p[p['brcid']] = p

    non_empty_concepts = []
    study_concepts = study_analyzer.study_concepts
    term_to_docs = {}
    ruled_anns = []
    for sc in study_concepts:
        positive_doc_anns = []
        sc_key = '%s(%s)' % (sc.name, len(sc.concept_closure))
        concept_list = ', '.join(['\'%s\'' % c for c in sc.concept_closure])
        doc_anns = []
        if len(sc.concept_closure) > 0:
            sql_temp = term_doc_anns_sql
            data_sql = sql_temp.format(**{'concepts': concept_list,
                                          'cohort_id': cohort_name,
                                          'extra_constrains':
                                              ' \n '.join(
                                                  [generate_skip_term_constrain(study_analyzer, skip_term_sql)]
                                                  + [] if (study_analyzer.study_options is None or
                                                           study_analyzer.study_options['extra_constrains'] is None)
                                                  else study_analyzer.study_options['extra_constrains'])})
            print data_sql
            dutil.query_data(data_sql, doc_anns,
                             dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        if len(doc_anns) > 0:
            p_to_dfreq = {}
            counted_docs = set()
            for ann in doc_anns:
                p = ann['brcid']
                d = ann['CN_Doc_ID']
                if d in counted_docs:
                    continue
                ruled, rule = rule_executor.execute(ann['TextContent'],
                                                    int(ann['start_offset']),
                                                    int(ann['end_offset']))
                if not ruled:
                    counted_docs.add(d)
                    p_to_dfreq[p] = 1 if p not in p_to_dfreq else 1 + p_to_dfreq[p]
                    positive_doc_anns.append({'id': ann['CN_Doc_ID'],
                                              'content': ann['TextContent'],
                                              'annotations': [{'start': ann['start_offset'],
                                                               'end': ann['end_offset'],
                                                               'concept': ann['inst_uri']}],
                                              'doc_table': ann['src_table'],
                                              'doc_col': ann['src_col']})
                else:
                    ruled_anns.append({'p': p, 'd': d, 'ruled': rule})
            if len(counted_docs) > 0:
                non_empty_concepts.append(sc_key)
                for p in p_to_dfreq:
                    id2p[p][sc_key] = str(p_to_dfreq[p])

                # save sample docs
                if sample_size >= len(positive_doc_anns):
                    term_to_docs[sc_key] = positive_doc_anns
                else:
                    sampled = []
                    for i in xrange(sample_size):
                        index = random.randrange(len(positive_doc_anns))
                        sampled.append(positive_doc_anns[index])
                        positive_doc_anns.pop(index)
                    term_to_docs[sc_key] = sampled

    concept_labels = sorted(non_empty_concepts)
    s = '\t'.join(['brcid'] + concept_labels) + '\n'
    for p in patients:
        s += '\t'.join([p['brcid']] + [p[k] if k in p else '0' for k in concept_labels]) + '\n'
    utils.save_string(s, out_file)
    utils.save_json_array(term_to_docs, sample_out_file, encoding='cp1252')
    utils.save_json_array(ruled_anns, ruled_ann_out_file, encoding='cp1252')
    print 'done'


def random_extract_annotated_docs(cohort_name, study_analyzer, out_file,
                                  docs_by_term_sql, docs_by_cohort_sql, docs_by_ids_sql,
                                  sample_size=5):

    term_to_docs = {}
    study_concepts = study_analyzer.study_concepts
    is_NOT_cohort_based = study_analyzer.study_options is None \
                          or study_analyzer.study_options['sample_non_hits'] is None \
                          or (not study_analyzer.study_options['sample_non_hits'])
    for sc in study_concepts:
        sc_key = '%s(%s)' % (sc.name, len(sc.concept_closure))
        concept_list = ', '.join(['\'%s\'' % c for c in sc.concept_closure])
        doc_ids = []
        if len(sc.concept_closure) > 0:
            if is_NOT_cohort_based:
                dutil.query_data(
                    docs_by_term_sql.format(
                        **{'concepts': concept_list,
                           'cohort_id': cohort_name,
                           'extra_constrains':
                               ' \n '.join(
                                   [generate_skip_term_constrain(study_analyzer)]
                                   + [] if (study_analyzer.study_options is None or
                                            study_analyzer.study_options['extra_constrains'] is None)
                                   else study_analyzer.study_options['extra_constrains'])}),
                    doc_ids)
            else:
                doc_sql = docs_by_cohort_sql.format(
                    **{'cohort_id': cohort_name,
                       'extra_constrains':
                           ' and '.join(
                               [generate_skip_term_constrain(study_analyzer)]
                               + [] if (study_analyzer.study_options is None or
                                        study_analyzer.study_options['extra_constrains'] is None)
                               else study_analyzer.study_options['extra_constrains'])})
                print doc_sql
                dutil.query_data(doc_sql, doc_ids)
        if len(doc_ids) > 0:
            sample_ids = []
            if len(doc_ids) <= sample_size:
                sample_ids = [r['CN_Doc_ID'] for r in doc_ids]
            else:
                for i in xrange(sample_size):
                    index = random.randrange(len(doc_ids))
                    sample_ids.append(doc_ids[index]['CN_Doc_ID'])
                    del doc_ids[index]
            doc_list = ', '.join(['\'%s\'' % d for d in sample_ids])
            docs = []
            doc_sample_sql = docs_by_ids_sql.format(**{'docs': doc_list,
                                                       'concepts': concept_list,
                                                       'extra_constrains': generate_skip_term_constrain(study_analyzer)})
            print doc_sample_sql
            dutil.query_data(doc_sample_sql,
                             docs)
            doc_objs = []
            prev_doc_id = ''
            doc_obj = None
            for d in docs:
                if prev_doc_id != d['CN_Doc_ID']:
                    doc_obj = {'id': d['CN_Doc_ID'], 'content': d['TextContent'], 'annotations': [],
                               'doc_table': d['src_table'], 'doc_col': d['src_col']}
                    doc_objs.append(doc_obj)
                    prev_doc_id = d['CN_Doc_ID']
                doc_obj['annotations'].append({'start': d['start_offset'],
                                               'end': d['end_offset'],
                                               'concept': d['inst_uri']})
            term_to_docs[sc.name] = doc_objs
        if not is_NOT_cohort_based:
            break
    utils.save_json_array(term_to_docs, out_file)
    print 'done'


def do_action_trans_docs(docs, nlp,
                         doc_ann_sql_template,
                         doc_content_sql_template,
                         action_trans_update_sql_template,
                         db_conn_file,
                         corpus_predictor):
    """
    do actionable transparency prediction on a batch of docs.
    this function is to supposed to be called in a single thread
    :param docs:
    :param nlp:
    :param doc_ann_sql_template:
    :param doc_content_sql_template:
    :param action_trans_update_sql_template:
    :param db_conn_file:
    :param corpus_predictor:
    :return:
    """
    self_nlp = tstg.load_mode('en')
    for doc_id in docs:
        doc_anns = []
        dutil.query_data(doc_ann_sql_template.format(doc_id['docid']),
                         doc_anns,
                         dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        doc_anns = [{'s': int(ann['s']), 'e': int(ann['e']),
                     'AnnId': str(ann['AnnId']), 'signed_label':'', 'gt_label':''} for ann in doc_anns]
        if len(doc_anns) == 0:
            continue
        doc_container = []
        dutil.query_data(doc_content_sql_template.format(doc_id['docid']),
                         doc_container,
                         dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        ptns = tstg.doc_processing(self_nlp,
                                   unicode(doc_container[0]['content']),
                                   doc_anns,
                                   doc_id['docid'])
        print 'doc %s read/model created, predicting...'
        for inst in ptns:
            acc = corpus_predictor.predcit(inst)
            anns = inst.annotations
            sql = action_trans_update_sql_template.format(**{'acc': acc, 'ann_id': anns[0]['AnnId']})
            dutil.query_data(sql, container=None, dbconn=dutil.get_db_connection_by_setting(db_conn_file))


def action_transparentise(cohort_name, db_conn_file,
                          cohort_doc_sql_template,
                          doc_ann_sql_template,
                          doc_content_sql_template,
                          action_trans_update_sql_template,
                          corpus_trans_file):
    """
    use actionable transparency model to create confidence value for each annotations;
    this method split all cohort documents into batches that are to processed in multiple threads
    :param cohort_name:
    :param db_conn_file:
    :param cohort_doc_sql_template:
    :param doc_ann_sql_template:
    :param doc_content_sql_template:
    :param action_trans_update_sql_template:
    :param corpus_trans_file:
    :return:
    """
    docs = []
    dutil.query_data(cohort_doc_sql_template.format(cohort_name), docs,
                     dbconn=dutil.get_db_connection_by_setting(db_conn_file))
    batch_size = 500
    batches = []
    for i in range(0, len(docs), batch_size):
        batches.append(docs[i:i+batch_size])
    nlp = tstg.load_mode('en')
    corpus_predictor = tssp.CorpusPredictor.load_corpus_model(corpus_trans_file)
    utils.multi_thread_tasking(batches, 1, do_action_trans_docs,
                               args=[nlp,
                                     doc_ann_sql_template,
                                     doc_content_sql_template,
                                     action_trans_update_sql_template,
                                     db_conn_file,
                                     corpus_predictor
                                     ])
    print 'all anns transparentised'


if __name__ == "__main__":
    # concepts = utils.load_json_data('./resources/Surgical_Procedures.json')
    # populate_patient_concept_table('dementia', concepts, 'dementia_cohorts.csv')
    # dump_doc_as_files('./hepc_data')
    pass
