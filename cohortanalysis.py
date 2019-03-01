import utils
import sqldbutils as dutil
import json
from os.path import join
import ontotextapi as oi
import random
import study_analyzer as sa
from ann_post_rules import AnnRuleExecutor
# import trans_anns.sentence_pattern as tssp
# import trans_anns.text_generaliser as tstg
import re
from analysis.semquery import SemEHRES
import analysis.cohort_analysis_helper as chelper
from datetime import datetime


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
                                            db_conn_file, text_preprocessing=False):
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
    positive_dumps = []
    skip_terms_list = [t.lower() for t in rule_executor.skip_terms]
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
                ruled = False
                case_instance = ''
                if not ruled:
                    # skip term rules
                    if 'string_orig' in ann and ann['string_orig'].lower() in skip_terms_list:
                        ruled = True
                        rule = 'skip-term'
                        case_instance = ann['string_orig']
                if not ruled:
                    # string orign rules - not used now
                    ruled, case_instance = rule_executor.execute_original_string_rules(
                        ann['string_orig'] if 'string_orig' in ann
                        else ann['TextContent'][int(ann['start_offset']):int(ann['end_offset'])])
                    rule = 'original-string-rule'
                if not ruled:
                    # post processing rules
                    ruled, case_instance, rule = \
                        rule_executor.execute(ann['TextContent'] if not text_preprocessing else
                                              preprocessing_text_befor_rule_execution(ann['TextContent']),
                                              int(ann['start_offset']),
                                              int(ann['end_offset']),
                                              string_orig=ann['string_orig'] if 'string_orig' in ann else None)
                    rule = 'semehr ' + rule
                if not ruled:
                    # bio-yodie labels
                    if 'experiencer' in ann:
                        if ann['experiencer'].lower() != 'patient' or \
                                ann['temporality'].lower() != 'recent' or \
                                ann['negation'].lower() != 'affirmed':
                            ruled = True
                            case_instance = '\t'.join([ann['experiencer'], ann['temporality'], ann['negation']])
                            rule = 'yodie'
                if ruled:
                    ruled_anns.append({'p': p, 'd': d, 'ruled': rule, 's': ann['start_offset'],
                                       'e': ann['end_offset'],
                                       'c': ann['inst_uri'],
                                       'case-instance': case_instance,
                                       'string_orig': ann['string_orig']
                                       })
                else:
                    counted_docs.add(d)
                    p_to_dfreq[p] = 1 if p not in p_to_dfreq else 1 + p_to_dfreq[p]
                    positive_doc_anns.append({'id': ann['CN_Doc_ID'],
                                              'content': ann['TextContent'],
                                              'annotations': [{'start': ann['start_offset'],
                                                               'end': ann['end_offset'],
                                                               'concept': ann['inst_uri'],
                                                               'string_orig': ann[
                                                                   'string_orig'] if 'string_orig' in ann else ''}],
                                              'doc_table': ann['src_table'],
                                              'doc_col': ann['src_col']})
                    positive_dumps.append({'p': p, 'd': d, 's': ann['start_offset'],
                                           'e': ann['end_offset'],
                                           'c': ann['inst_uri'],
                                           'string_orig': ann['string_orig']})
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
    utils.save_string('var sample_docs=' + json.dumps(convert_encoding(term_to_docs, 'cp1252', 'utf-8')), sample_out_file)
    utils.save_json_array(convert_encoding(ruled_anns, 'cp1252', 'utf-8'), ruled_ann_out_file)
    utils.save_json_array(positive_dumps, out_file + "_json")
    print 'done'


def patient_timewindow_filter(fo, doc_id, pid):
    fes = fo['es']
    d = fes.get_doc_detail(str(doc_id))
    if d is None:
        print '%s/%s not found in full index' % (doc_id, pid)
        return True
    win = fo['p2win'][pid]
    if win is None:
        return False
    d_date = datetime.strptime(d[fo['date_field']][0:19], fo['date_format'])
    print d_date, win
    t0 = datetime.strptime(win['t0'], fo['pt_date_format'])
    t1 = datetime.strptime(win['t1'], fo['pt_date_format'])
    if t0 < d_date <= t1:
        return False
    else:
        print '%s filtered [%s]' % (doc_id, d_date) 
        return True


def es_populate_patient_study_table_post_ruled(study_analyzer, out_file, rule_executor,
                                               sample_size, sample_out_file, ruled_ann_out_file,
                                               es_conn_file, text_preprocessing=False,
                                               retained_patients_filter=None,
                                               filter_obj=None):
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
    es = SemEHRES.get_instance_by_setting_file(es_conn_file)
    if filter_obj is not None:
        fes = SemEHRES.get_instance_by_setting_file(filter_obj['doc_es_setting'])
        patient_id_field = filter_obj['patient_id_field']
        filter_obj['es'] = fes
    if retained_patients_filter is None:
        pids = es.search_by_scroll("*", es.patient_type)
    else:
        pids = retained_patients_filter
    patients = [{'brcid': p} for p in pids]
    id2p = {}
    for p in patients:
        id2p[p['brcid']] = p
    print 'total patients is %s' % len(patients)
    non_empty_concepts = []
    study_concepts = study_analyzer.study_concepts
    term_to_docs = {}
    ruled_anns = []
    for sc in study_concepts:
        positive_doc_anns = []
        sc_key = '%s(%s)' % (sc.name, len(sc.concept_closure))
        print 'working on %s' % sc_key
        if sc.name.startswith('ess_'):
            non_empty_concepts.append(sc_key)
            # elasticsearch concepts
            p2docs = chelper.query_doc_by_search(es, fes, sc.concept_closure, patient_id_field,
                                                 retained_patients_filter=retained_patients_filter,
                                                 filter_obj=filter_obj, doc_filter_function=patient_timewindow_filter)
            for pd in p2docs:
                id2p[pd['pid']][sc_key] = str(len(pd['docs']))
            # continue without to do the rest
            continue

        doc_anns = []
        if len(sc.concept_closure) > 0:
            doc_anns = chelper.query_doc_anns(es, sc.concept_closure, study_analyzer.skip_terms,
                                              retained_patients_filter=retained_patients_filter,
                                              filter_obj=filter_obj, doc_filter_function=patient_timewindow_filter
                                              )

        if len(doc_anns) > 0:
            p_to_dfreq = {}
            counted_docs = set()
            for d in doc_anns:
                doc = doc_anns[d]
                p = doc['pid']
                if d in counted_docs:
                    continue
                for ann in doc['anns']:
                    ruled, rule = rule_executor.execute(doc['text'] if not text_preprocessing else
                                                        preprocessing_text_befor_rule_execution(doc['text']),
                                                        int(ann['s']),
                                                        int(ann['e']))
                    if not ruled:
                        counted_docs.add(d)
                        p_to_dfreq[p] = 1 if p not in p_to_dfreq else 1 + p_to_dfreq[p]
                        positive_doc_anns.append({'id': d,
                                                  'content': doc['text'],
                                                  'annotations': [{'start': ann['s'],
                                                                   'end': ann['e'],
                                                                   'concept': ann['inst']}]})
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
    utils.save_json_array(convert_encoding(term_to_docs, 'cp1252', 'utf-8'), sample_out_file)
    utils.save_json_array(convert_encoding(ruled_anns, 'cp1252', 'utf-8'), ruled_ann_out_file)
    print 'done'


def preprocessing_text_befor_rule_execution(t):
    return re.sub(r'\s{2,}', ' ', t)


def convert_encoding(dic_obj, orig, target):
    if isinstance(dic_obj, str):
        return dic_obj.decode(orig).encode(target)
    elif isinstance(dic_obj, list):
        ret = []
        for itm in dic_obj:
            ret.append(convert_encoding(itm, orig, target))
        return ret
    elif isinstance(dic_obj, dict):
        for k in dic_obj:
            dic_obj[k] = convert_encoding(dic_obj[k], orig, target)
        return dic_obj
    else:
        # print '%s not supported for conversion' % isinstance(dic_obj[k], dict)
        return dic_obj


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
    # self_nlp = tstg.load_mode('en')
    for doc_id in docs:
        doc_anns = []
        dutil.query_data(doc_ann_sql_template.format(doc_id['docid']),
                         doc_anns,
                         dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        doc_anns = [{'s': int(ann['s']), 'e': int(ann['e']),
                     'AnnId': str(ann['AnnId']), 'signed_label':'', 'gt_label':'', 'action_trans': ann['action_trans']} for ann in doc_anns]
        if len(doc_anns) == 0:
            continue
        if doc_anns[0]['action_trans'] is not None:
            print 'found trans %s of first ann, skipping doc' % doc_anns[0]['action_trans']
            continue
        doc_container = []
        dutil.query_data(doc_content_sql_template.format(doc_id['docid']),
                         doc_container,
                         dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        ptns = tstg.doc_processing(nlp,
                                   unicode(doc_container[0]['content']),
                                   doc_anns,
                                   doc_id['docid'])
        # print 'doc %s read/model created, predicting...'
        for inst in ptns:
            acc = corpus_predictor.predcit(inst)
            anns = inst.annotations
            sql = action_trans_update_sql_template.format(**{'acc': acc, 'AnnId': anns[0]['AnnId']})
            # print 'executing %s' % sql
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
    for batch in batches:
        print 'working on %s/%s batch' % (i, len(batches))
        try:
            do_action_trans_docs(batch, 
                                 nlp,
                                 doc_ann_sql_template,
                                 doc_content_sql_template,
                                 action_trans_update_sql_template,
                                 db_conn_file,
                                 corpus_predictor)
        except Exception as e:
            print 'error processing [%s]' % e
        i += 1
    #utils.multi_thread_tasking(batches, 1, do_action_trans_docs,
    #                           args=[nlp,
    #                                 doc_ann_sql_template,
    #                                 doc_content_sql_template,
    #                                 action_trans_update_sql_template,
    #                                 db_conn_file,
    #                                 corpus_predictor
    #                                 ])
    print 'all anns transparentised'


def do_put_line(p, concept_labels, container):
    print 'working on %s' % p['brcid']
    container.append('\t'.join([p['brcid']] + [str(p[k]) if k in p else '0' for k in concept_labels]))


def generate_result_in_one_iteration(cohort_name, study_analyzer, out_file,
                                     sample_size, sample_out_file,
                                     doc_to_brc_sql, brc_sql, anns_iter_sql, skip_term_sql, doc_content_sql,
                                     db_conn_file):
    """
    generate result in one iteration over all annotations. this is supposed to be much faster when working on
    large study concepts. But post-processing using rules not supported now
    :param cohort_name:
    :param study_analyzer:
    :param out_file:
    :param sample_size:
    :param sample_out_file:
    :param doc_to_brc_sql:
    :param brc_sql:
    :param anns_iter_sql:
    :param skip_term_sql:
    :param doc_content_sql:
    :param db_conn_file:
    :return:
    """
    # populate concept to anns maps
    sc2anns = {}
    for sc in study_analyzer.study_concepts:
        sc2anns[sc.name] = []

    # populate patient list
    print 'populating patient list...'
    patients = {}
    rows_container = []
    dutil.query_data(brc_sql.format(cohort_name), rows_container,
                     dbconn=dutil.get_db_connection_by_setting(db_conn_file))
    for r in rows_container:
        patients[r['brcid']] = {'brcid': r['brcid']}

    # populate document id to patient id dictionary
    print 'populating doc to patient map...'
    rows_container = []
    dutil.query_data(doc_to_brc_sql.format(cohort_name), rows_container,
                     dbconn=dutil.get_db_connection_by_setting(db_conn_file))
    doc2brc = {}
    for dp in rows_container:
        doc2brc[dp['doc_id']] = dp['brcid']

    # query annotations
    print 'iterating annotations...'
    rows_container = []
    dutil.query_data(anns_iter_sql.format(**{'cohort_id': cohort_name,
                                             'extra_constrains':
                                                 ' \n '.join(
                                                  [generate_skip_term_constrain(study_analyzer, skip_term_sql)]
                                                  + [] if (study_analyzer.study_options is None or
                                                           study_analyzer.study_options['extra_constrains'] is None)
                                                  else study_analyzer.study_options['extra_constrains'])}),
                     rows_container,
                     dbconn=dutil.get_db_connection_by_setting(db_conn_file))
    for r in rows_container:
        concept_id = r['inst_uri']
        brcid = doc2brc[r['doc_id']] if r['doc_id'] in doc2brc else None
        if brcid is None:
            print 'doc %s not matched to a patient!!!' % r['doc_id']
            continue
        patient = patients[brcid] if brcid in patients else None
        if patient is None:
            print 'brc id %s not matched a patient!!!' % brcid
            continue
        # get matched study concepts
        for sc in study_analyzer.study_concepts:
            if concept_id in sc.concept_closure:
                patient[sc.name] = (patient[sc.name] + 1) if sc.name in patient else 1
                sc2anns[sc.name].append({'ann_id': r['ann_id'], 'doc_id': r['doc_id'], 'concept_id': concept_id,
                                         'start': r['start_offset'], 'end': r['end_offset']})

    # generate result table
    print 'generate result table...'
    concept_labels = sorted([k for k in sc2anns])
    s = '\t'.join(['brcid'] + concept_labels) + '\n'
    lines = []
    utils.multi_thread_tasking([patients[pid] for pid in patients], 40, do_put_line,
                               args=[concept_labels, lines])
    s += '\n'.join(lines)
    utils.save_string(s, out_file)

    # generate sample annotations
    term_to_docs = {}
    for concept in sc2anns:
        ann_ids = sc2anns[concept]
        sample_ids = []
        if len(ann_ids) <= sample_size:
            sample_ids = ann_ids
        else:
            for i in xrange(sample_size):
                index = random.randrange(len(ann_ids))
                sample_ids.append(ann_ids[index])
                del ann_ids[index]
        term_to_docs[concept] = sample_ids

    # query doc contents
    print 'populating term to sampled anns...'
    term_to_sampled = {}
    for term in term_to_docs:
        sample_ids = term_to_docs[term]
        if len(sample_ids) <=0 :
            continue
        sample_doc_ids = ['\'' + s['doc_id'] + '\'' for s in sample_ids]
        rows_container = []
        dutil.query_data(doc_content_sql.format(','.join(sample_doc_ids)), rows_container,
                         dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        doc_to_content = {}
        for r in rows_container:
            doc_to_content[r['doc_id']] = r['TextContent']
        term_sampled = []
        for s in sample_ids:
            term_sampled.append({'id': s['doc_id'],
                                 'content': doc_to_content[s['doc_id']],
                                 'annotations': [{'start': s['start'],
                                                  'end': s['end'],
                                                  'concept': s['concept_id']}]})
        term_to_sampled[term] = term_sampled
    utils.save_json_array(convert_encoding(term_to_sampled, 'cp1252', 'utf-8'), sample_out_file)


def complete_sample_ann_data(key_anns, complete_sql, db_conn_file, container):
    k = key_anns[0]
    anns = key_anns[1]
    for ann in anns:
        rows_container = []
        dutil.query_data(complete_sql.format(**{'doc_id': ann['id'],
                                                'start': ann['annotations'][0]['start'],
                                                'end': ann['annotations'][0]['end'],
                                                'concept': ann['annotations'][0]['concept']}),
                         rows_container,
                         dbconn=dutil.get_db_connection_by_setting(db_conn_file))
        if len(rows_container) > 0:
            ann['annotations'][0]['string_orig'] = rows_container[0]['string_orig']
            if 'action_trans' in rows_container[0]:
                ann['annotations'][0]['confidence'] = rows_container[0]['action_trans']
    container.append([k, anns])


def complete_samples(sample_file, complete_sql, db_conn_file, out_file):
    ann_prefix = 'var sample_docs='
    anns_str = utils.read_text_file_as_string(sample_file)
    if anns_str.startswith(ann_prefix):
        anns_str = anns_str[len(ann_prefix):]
    anns = json.loads(anns_str)
    # anns = utils.load_json_data(sample_file)
    key_anns = []
    for k in anns:
        key_anns.append((k, anns[k]))
    container = []
    utils.multi_thread_tasking(key_anns, 40, complete_sample_ann_data,
                               args=[complete_sql, db_conn_file, container])
    results = {}
    for r in container:
        results[r[0]] = r[1]
    utils.save_string(ann_prefix + json.dumps(results), out_file)
    print 'done'


def test_es_analysis():
    study_analyzer = sa.StudyAnalyzer('aaa')
    study_concept = sa.StudyConcept('depression', ['depression'])
    study_analyzer.skip_terms = ['Recurrent major depressive episodes']
    study_concept.concept_closure = ["C0154409", "C0038050"]
    study_analyzer.add_concept(study_concept)

    folder = "./studies/COMOB_SD/"
    ruler = AnnRuleExecutor()
    rules = utils.load_json_data(join(folder, 'post_filter_rules.json'))
    for r in rules:
        ruler.add_filter_rule(r['offset'], r['regs'])

    es_populate_patient_study_table_post_ruled(study_analyzer, "./out.txt", ruler, 20,
                                               "./sample_out.txt", "./ruled.txt",
                                               "./index_settings/sem_idx_setting.json")


if __name__ == "__main__":
    test_es_analysis()
