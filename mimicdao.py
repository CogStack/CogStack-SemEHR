# mimic III data access functions
# - postgres library is needed
# Honghan 2016

import psycopg2
import datetime
import json

# db connection string
db_cnn_str = "dbname='mimic' user='mimic' password='123321!a' host='10.200.100.216'"

# query templates
# patient cohort by diagnosis template
qt_patient_cohort = """

select distinct p.*
from mimiciii.diagnoses_icd d, mimiciii.patients p
where d.icd9_code = '{}'
and p.subject_id=d.subject_id
"""

# hospital admissions by diagnosis template
qt_admissions_by_diagnosis = """
select distinct a.hadm_id, a.admittime, a.dischtime, a.deathtime
from mimiciii.diagnoses_icd d, mimiciii.admissions a
where d.icd9_code = '{}'
and d.hadm_id=a.hadm_id
"""

# get admission labevents
qt_labevents_by_admssion = """
select l.*, d.label, d.category
from mimiciii.labevents l, mimiciii.d_labitems d
where hadm_id={}
and l.itemid = d.itemid
order by l.itemid, charttime
"""

# get admission chart events
qt_chartevents_by_admssion = """
select l.*, d.label, d.category
from mimiciii.chartevents l, mimiciii.d_items d
where hadm_id={}
and l.itemid = d.itemid
order by l.itemid, charttime
"""

# get procedure events
qt_procdure_by_admssion = """
select i.label, p.*
from mimiciii.procedureevents_mv p, mimiciii.d_items i
where p.hadm_id={} and p.itemid=i.itemid
order by starttime
"""

# get mimic doc (noteevents) by rowid
mimic_doc_by_row_id = """
 select text, subject_id, hadm_id, extract(epoch from chartdate) chartdate, charttime from mimiciii.noteevents where row_id={doc_id}
"""

# get distinct patient ids
mimic_patient_ids = """
 select distinct subject_id from mimiciii.patients
"""

# get doc type
mimic_doc_types = """
 select row_id, category from mimiciii.noteevents 
"""

# calculate history dates
mimic_doc_shift_date = """
 select row_id, chartdate, chartdate::date - 365 * 200 thedate from mimiciii.noteevents
"""

# get discharge summary for a patient
mimic_doc_discharge_summary = """
 select row_id from mimiciii.noteevents where subject_id='{patient_id}' and category='Discharge summary' order by chartdate desc limit 1
"""

# get patient lab events
mimic_patient_labevents = """
 select e.value, i.label from mimiciii.labevents e left join mimiciii.d_labitems i on e.itemid=i.itemid where subject_id='{patient_id}' 
"""

# create db connection
def get_db_connection():
    db_conn = psycopg2.connect(db_cnn_str)
    cursor = db_conn.cursor()
    return {'db_conn': db_conn, 'cursor': cursor}


# release connection resources
def release_db_connection(cnn_obj):
    cnn_obj['cursor'].close()
    cnn_obj['db_conn'].commit()
    cnn_obj['db_conn'].close()


# query db to get data as a list of dic objects
def query_data(query, container):
    """
    query db to get data
    :param query: sql query
    :param container: the list container to save each row as a dic object
    :return:
    """
    conn_dic = get_db_connection()
    conn_dic['cursor'].execute(query)
    rows = conn_dic['cursor'].fetchall()
    columns = [column[0] for column in conn_dic['cursor'].description]
    for row in rows:
        obj = dict(zip(columns, row))
        for k in obj:
            if type(obj[k]) is datetime.datetime:
                obj[k] = str(obj[k].strftime("%Y-%m-%d %H:%M:%S"))
        container.append(obj)
    release_db_connection(conn_dic)


def get_patient_cohort(icd9_code):
    patients = []
    query_data(qt_patient_cohort.format(icd9_code), patients)
    return patients


def get_admissions(icd9_code):
    adms = []
    query_data(qt_admissions_by_diagnosis.format(icd9_code), adms)
    return adms


def get_events_by_admission(event_query_tempalte, hadm_id):
    events = []
    query_data(event_query_tempalte.format(hadm_id), events)
    return events


def get_labevents_by_admission(hadm_id):
    return get_events_by_admission(qt_labevents_by_admssion, hadm_id)


def get_chartevents_by_admission(hadm_id):
    return get_events_by_admission(qt_chartevents_by_admssion, hadm_id)


def get_procedures_by_admission(hadm_id):
    return get_events_by_admission(qt_procdure_by_admssion, hadm_id)


def get_mimic_doc_by_id(doc_id):
    sql = mimic_doc_by_row_id.format(**{'doc_id': doc_id})
    docs = []
    query_data(sql, docs)
    return docs


def get_all_patient_ids():
    patients = []
    query_data(mimic_patient_ids, patients)
    return patients


def get_doc_types():
    docs = []
    query_data(mimic_doc_types, docs)
    return docs


def get_doc_dates():
    docs = []
    query_data(mimic_doc_shift_date, docs)
    return docs


def get_summary_doc_by_patient(patient_id):
    docs = []
    query_data(mimic_doc_discharge_summary.format(**{'patient_id': patient_id}), docs)
    return docs


def get_patient_labevents(patient_id):
    docs = []
    query_data(mimic_patient_labevents.format(**{'patient_id': patient_id}), docs)
    l2v = {}
    for e in docs:
        l2v[e['label']] = l2v[e['label']] + [e['value']] if e['label'] in l2v else [e['value']]
    return l2v

if __name__ == "__main__":
    # adms = get_admissions('99592')
    # print '#admissions with diagnose of severe sepsis {}'.format(len(adms))
    # print json.dumps(get_labevents_by_admission(adms[0]['hadm_id']))
    print get_patient_labevents('212')
