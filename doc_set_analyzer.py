import utils
import sqldbutils as dutil
import json
from os.path import join, isfile, split
from study_analyzer import StudyAnalyzer, StudyConcept
from datetime import datetime


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


def populate_episode_study_table(study_analyzer, episode_data, out_path):
    study_concepts = study_analyzer.study_concepts
    for sc in study_concepts:
        sc_key = '%s(%s)' % (sc.name, len(sc.concept_closure))
        print 'working on %s' % sc_key
        concept_list = ', '.join(['\'%s\'' % c for c in sc.concept_closure])
        patient_date_tuples = []
        if len(sc.concept_closure) > 0:
            data_sql = doc_concept_sql.format(**{'concepts': concept_list,
                                                 'extra_constrains': ''})
            print data_sql
            dutil.query_data(data_sql, patient_date_tuples)
            # filter patient_date tuples using episode constraints
            for eps in episode_data:
                for row in patient_date_tuples:
                    if eps['brcid'] == row['brcid']:
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
        utils.save_string(s, out_path + '/eps' + w + '.tsv')


def count_eps_win(eps, concept_name, row, win):
    if eps[win]['s'] <= row['DateModified'] <= eps[win]['e']:
        eps[concept_name][win] += 1


def load_episode_data(file_path):
    lines = utils.read_text_file(file_path)
    eps = []
    for l in lines:
        arr = l.split('\t')
        eps.append({'brcid': arr[0],
                    'win1': {'s': datetime.strptime(arr[1], '%d/%m/%Y %H:%M'), 'e': datetime.strptime(arr[2], '%d/%m/%Y %H:%M')},
                    'win2': {'s': datetime.strptime(arr[3], '%d/%m/%Y %H:%M'), 'e': datetime.strptime(arr[4], '%d/%m/%Y %H:%M')},
                    'win3': {'s': datetime.strptime(arr[5], '%d/%m/%Y %H:%M'), 'e': datetime.strptime(arr[6], '%d/%m/%Y %H:%M')}
                    })
    return eps


def study(folder, episode_file):
    episodes = load_episode_data(episode_file)
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
    populate_episode_study_table(sa, episodes, './resources')
    print 'done'


if __name__ == "__main__":
    study('./studies/clozapine', 'studies/clozapine/episodes.txt')

