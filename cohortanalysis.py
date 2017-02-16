import utils
import sqldbutils as dutil
import json
import ontotextapi as oi
from study_analyzer import StudyAnalyzer

# query concept sql
autoimmune_concepts_sql = """
select distinct concept_name from [SQLCRIS_User].[Kconnect].[ulms_concept_mapping]
"""

# query patient sql
patients_sql = """
select brcid from
[SQLCRIS_User].[Kconnect].[cohorts]
where patient_group='{}'
"""

# query concept freqs over patient
concept_doc_freq_sql = """
  select c.brcid, COUNT(distinct a.CN_Doc_ID) num
  from [SQLCRIS_User].[Kconnect].[cohorts] c, [SQLCRIS_User].Kconnect.kconnect_annotations a, GateDB_Cris.dbo.gate d
  where
  a.inst_uri='{0}'
  and a.CN_Doc_ID = d.CN_Doc_ID
  and c.brcid = d.BrcId
  and c.patient_group='{1}'
  group by c.brcid
"""

# query term (potentially represented by a list of concepts) freqs over patient
term_doc_freq_sql = """
  select p.brcid, COUNT(distinct a.CN_Doc_ID) num
  from [SQLCRIS_User].[Kconnect].[cohorts] c, [SQLCRIS_User].Kconnect.kconnect_annotations a, GateDB_Cris.dbo.gate d
  where
  a.inst_uri in ({0})
  and a.CN_Doc_ID = d.CN_Doc_ID
  and c.brcid = d.BrcId
  and c.patient_group='{1}'
  group by c.brcid
"""


def populate_patient_concept_table(cohort_name, concepts, out_file):
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


def populate_patient_study_table(cohort_name, study_analyzer, out_file):
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
        sc_key = '%(%)' % (sc.name, len(sc.concept_closure))
        concept_list = ', '.join(['\'%\'' % c for c in sc.concept_closure])
        patient_term_freq = []
        dutil.query_data(term_doc_freq_sql.format(concept_list, cohort_name), patient_term_freq)
        if len(patient_term_freq) > 0:
            non_empty_concepts.append(sc_key)
            for pc in patient_term_freq:
                id2p[pc['brcid']][sc_key] = pc['num']

    concept_labels = sorted(non_empty_concepts)
    s = '\t'.join(['brcid'] + concept_labels) + '\n'
    for p in patients:
        s += '\t'.join([p['brcid']] + [p[k] for k in concept_labels]) + '\n'
    utils.save_string(s, out_file)
    print 'done'


if __name__ == "__main__":
    concepts = utils.load_json_data('./resources/Surgical_Procedures.json')
    populate_patient_concept_table('dementia', concepts, 'dementia_cohorts.csv')
