import utils
import sqldbutils as dutil
import json
from os.path import join, isfile, split
from study_analyzer import StudyAnalyzer, StudyConcept
from datetime import datetime
import joblib as jl


my_host = 'localhost'
my_user = ''
my_pwd = ''
my_db = ''
my_sock = ''


db_connection = 'sql'


# get anns by doc set and concepts
doc_concept_sql = """
  select d.brcid, d.DateModified
  from kconnect_annotations a, working_docs d
  where
  a.inst_uri in ({concepts})
  and a.CN_Doc_ID = d.CN_Doc_ID
  and a.experiencer = 'Patient' and a.negation='Affirmed' and a.temporality = 'Recent'
  {extra_constrains}
"""

# get anns by doc set and concepts
doc_concept_sql_cohort = """
  select d.brcid, d.DateModified
  from sqlcris_user.kconnect.kconnect_annotations a, GateDB_Cris.dbo.gate d,  sqlcris_user.kconnect.cohorts c
  where
  a.inst_uri in ({concepts})
  and a.CN_Doc_ID = d.CN_Doc_ID
  and a.experiencer = 'Patient' and a.negation='Affirmed' and a.temporality = 'Recent'
  and c.patient_group='{cohort}'
  and d.brcid=c.brcid
  {extra_constrains}
"""



# load all brcid, docid, date in one go
patient_doc_date_sql = """
  select d.brcid, d.cn_doc_id, d.date from gate_attachment d, kconnect.dbo.cohorts c
  where d.brcid=c.brcid and c.patient_group in ({patient_groups})
  union all
  select d.brcid, d.cn_doc_id, d.date from gate_remaning_src_table d, kconnect.dbo.cohorts c
  where d.brcid=c.brcid and c.patient_group in ({patient_groups})
"""


update_doc_date_sql = """
  update working_docs set datemodified='{date}' where cn_doc_id='{doc_id}'
"""

def populate_episode_study_table(study_analyzer, episode_data, out_path, cohort):
    study_concepts = study_analyzer.study_concepts
    for sc in study_concepts:
        sc_key = '%s(%s)' % (sc.name, len(sc.concept_closure))
        print 'working on %s' % sc_key
        concept_list = ', '.join(['\'%s\'' % c for c in sc.concept_closure])
        patient_date_tuples = []
        if len(sc.concept_closure) > 0:
            data_sql = doc_concept_sql_cohort.format(**{'concepts': concept_list,
                                                     'cohort': cohort,
                                                     'extra_constrains': ''})
            print data_sql
            dutil.query_data(data_sql, patient_date_tuples,
                             dbconn=dutil.get_mysqldb_connection(my_host, my_user, my_pwd, my_db, my_sock)
                             if db_connection == 'mysql' else None)
            # filter patient_date tuples using episode constraints
            for eps in episode_data:
                for row in patient_date_tuples:
                    if eps['brcid'] == str(row['brcid']):
                        eps[sc.name] = {'win1': 0, 'win2': 0, 'win3': 0} if sc.name not in eps else eps[sc.name]
                        count_eps_win(eps, sc.name, row, 'win1')
                        count_eps_win(eps, sc.name, row, 'win2')
                        count_eps_win(eps, sc.name, row, 'win3')
    rows = []
    headers = ['brcid'] + [sc.name for sc in study_concepts]
    for eps in episode_data:
        r = {'win1':[eps['brcid']], 'win2':[eps['brcid']], 'win3':[eps['brcid']]}
        for sc in study_concepts:
            if sc.name in eps:
                r['win1'].append(str(eps[sc.name]['win1']))
                r['win2'].append(str(eps[sc.name]['win2']))
                r['win3'].append(str(eps[sc.name]['win3']))
            else:
                r['win1'].append('0')
                r['win2'].append('0')
                r['win3'].append('0')
        rows.append(r)

    for w in ['win1', 'win2', 'win3']:
        s = '\t'.join(headers) + '\n'
        for r in rows:
            s += '\t'.join(r[w]) + '\n'
        utils.save_string(s, out_path + '/weeks_eps' + w + '_control.tsv')


def count_eps_win(eps, concept_name, row, win):
    if eps[win]['s'] <= datetime.strptime(row['DateModified'], '%d/%m/%Y') <= eps[win]['e']:
        eps[concept_name][win] += 1


def load_episode_data(file_path, date_format='%d/%m/%Y %H:%M'):
    lines = utils.read_text_file(file_path)
    eps = []
    for l in lines:
        arr = l.split('\t')
        eps.append({'brcid': arr[0],
                    'win1': {'s': datetime.strptime(arr[1], date_format), 'e': datetime.strptime(arr[2], date_format)},
                    'win2': {'s': datetime.strptime(arr[3], date_format), 'e': datetime.strptime(arr[4], date_format)},
                    'win3': {'s': datetime.strptime(arr[5], date_format), 'e': datetime.strptime(arr[6], date_format)}
                    })
    return eps


def study(folder, episode_file, cohort, date_format='%d/%m/%Y %H:%M'):
    episodes = load_episode_data(episode_file, date_format=date_format)
    p, fn = split(folder)
    if isfile(join(folder, 'study_analyzer.pickle')):
        sa = StudyAnalyzer.deserialise(join(folder, 'study_analyzer.pickle'))
    else:
        sa = StudyAnalyzer(fn)
        if isfile(join(folder, 'exact_concepts_mappings.json')):
            concept_mappings = utils.load_json_data(join(folder, 'exact_concepts_mappings.json'))
            scs = []
            for t in concept_mappings:
                sc = StudyConcept(t, [t])
                t_c = {}
                t_c[t] = [concept_mappings[t]]
                sc.gen_concept_closure(term_concepts=t_c)
                scs.append(sc)
                print sc.concept_closure
            sa.study_concepts = scs
            sa.serialise(join(folder, 'study_analyzer.pickle'))
        else:
            concepts = utils.load_json_data(join(folder, 'study_concepts.json'))
            if len(concepts) > 0:
                scs = []
                for name in concepts:
                    scs.append(StudyConcept(name, concepts[name]))
                    print name, concepts[name]
            sa.study_concepts = scs
            sa.serialise(join(folder, 'study_analyzer.pickle'))

    # compute disjoint concepts
    sa.generate_exclusive_concepts()
    merged_mappings = {}
    for c in sa.study_concepts:
        for t in c.term_to_concept:
            all_concepts = list(c.concept_closure)
            if len(all_concepts) > 1:
                idx = 0
                for cid in all_concepts:
                    merged_mappings['(%s) %s (%s)' % (c.name, t, idx)] = {'closure': len(all_concepts), 'mapped': cid}
                    idx += 1
            else:
                merged_mappings['(%s) %s' % (c.name, t)] = c.term_to_concept[t]
        print c.name, c.term_to_concept, c.concept_closure
        print json.dumps(list(c.concept_closure))
    print json.dumps(merged_mappings)
    print 'generating result table...'
    populate_episode_study_table(sa, episodes, './resources', cohort)
    print 'done'


def dump_patient_doc_date_data(patient_groups, out_file):
    data_sql = patient_doc_date_sql.format(**{'patient_groups': patient_groups})
    d = []
    dutil.query_data(data_sql, d)
    jl.dump(d, out_file)


def update_doc_date(cnn_obj, data):
    sqls = []
    for r in data:
        if r['date'] is not None:
            sqls.append(update_doc_date_sql.format(**{'date': r['date'],
                                                  'doc_id': r['cn_doc_id']}))
    if len(sqls) > 0:
        cursor = cnn_obj['cursor']
        for sql in sqls:
            cursor.execute(sql)
        cnn_obj['cnxn'].commit()


def get_mysql_conn():
    return dutil.get_mysqldb_connection(my_host, my_user, my_pwd, my_db, my_sock)


def update_doc_dates(ser_file):
    data = jl.load(ser_file)
    step = 100
    batch = []
    for i in xrange(0, len(data), step):
        batch.append(data[i:min(len(data), i + step)])
    utils.multi_thread_tasking(batch, 20, update_doc_date,
                               thread_init_func=get_mysql_conn,
                               thread_end_func=dutil.release_db_connection)

if __name__ == "__main__":
    # study('./studies/clozapine', 'studies/clozapine/episodes.txt')
    study('./studies/clozapine', 'studies/clozapine/control_episodes_weeks.tsv', 'unknown_clozapine_sep2017', '%Y-%m-%d %H:%M:%S')
    # dump_patient_doc_date_data('\'clozapine_4k\'', 'resource/clozapine_4k_doc_map.pickle')
    # update_doc_dates('resources/clozapine_4k_doc_map.pickle')

