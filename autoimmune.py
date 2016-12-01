import utils
import sqldbutils as dutil

# query concept sql
autoimmune_concepts_sql = """
select distinct concept_name from [SQLCRIS_User].[Kconnect].[ulms_concept_mapping]
"""

# query patient sql
patients_sql = """
select brcid, primary_diag, diagnosis_date, dob, gender_id, ethnicitycleaned from [SQLCRIS_User].[Kconnect].[treatment_res_dep]
"""

# query concept freqs over patient
autoimmune_sympton_freq_sql = """
  select p.brcid, COUNT(distinct a.CN_Doc_ID) num 
  from [SQLCRIS_User].Kconnect.treatment_res_dep p, [SQLCRIS_User].Kconnect.kconnect_annotations a, GateDB_Cris.dbo.gate d, [SQLCRIS_User].Kconnect.[ulms_concept_mapping] c
  where 
  a.inst_uri=c.concept_id
  and a.CN_Doc_ID = d.CN_Doc_ID
  and p.brcid = d.BrcId
  and c.concept_name='{}'
  group by p.brcid
"""


def get_concepts(output_file):
    autoimmune_concepts = []
    patients = []
    dutil.query_data(autoimmune_concepts_sql, autoimmune_concepts)
    print '{} concepts read'.format(len(autoimmune_concepts))
    dutil.query_data(patients_sql, patients)
    print patients[0]
    # patient dic
    patient_dic = {}
    for p in patients:
        patient_dic[p['brcid']] = p

    for co in autoimmune_concepts:
        c = co['concept_name']
        sympton_freq_result = []
        print autoimmune_sympton_freq_sql.format(c)
        dutil.query_data(autoimmune_sympton_freq_sql.format(c), sympton_freq_result)
        for sf in sympton_freq_result:
            patient_dic[sf['brcid']][c] = sf['num']
    p_attrs = ['brcid', 'primary_diag', 'diagnosis_date', 'dob', 'gender_id', 'ethnicitycleaned']
    d_attrs = sorted([co['concept_name'] for co in autoimmune_concepts])
    s = '\t'.join(p_attrs) + '\t' + '\t'.join(d_attrs) + '\n'
    for p in patients:
        s += '\t'.join([str(p[k]) for k in p_attrs]) + '\t' + '\t'.join(['0' if c not in p else str(p[c]) for c in d_attrs]) + '\n'
    utils.save_string(s, output_file)


if __name__ == "__main__":
    get_concepts('autoimmune_results.csv')
