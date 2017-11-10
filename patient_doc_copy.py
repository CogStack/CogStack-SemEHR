import entity_centric_es as ees
import sys
import urllib3
from datetime import date, datetime
import datetime
import utils
import json
from os.path import isfile
import sqldbutils


job_sql_template = """
 select ...
 where updated_time > '{start_time_point}' and updated_time <= '{end_time_point}'
"""


class JobStatus(object):
    STATUS_SUCCESS = 0
    STATUS_FAILURE = -1
    STATUS_WORKING = 1
    STATUS_UNKNOWN = -2

    def __init__(self, job_file, dfmt='%Y-%m-%d %H:%M:%S'):
        self._dfmt = dfmt
        self._end_time_point = None
        self._start_time_point = None
        self._last_status = JobStatus.STATUS_FAILURE
        self._job_file = job_file
        self.load_data()

    def load_data(self):
        if isfile(self._job_file):
            d = utils.load_json_data(self._job_file)
            self._end_time_point = d['end_time_point']
            self._start_time_point = d['start_time_point']
            self._last_status = d['last_status']
        else:
            self._end_time_point = datetime.datetime.now().strftime(self._dfmt)
            self._start_time_point = datetime.date(2000, 1, 1).strftime(self._dfmt)
            self._last_status = JobStatus.STATUS_UNKNOWN

    def save(self):
        utils.save_json_array(self.get_ser_data(), self._job_file)

    def get_ser_data(self):
        return {'last_status': self._last_status,
                'start_time_point': self._start_time_point,
                'end_time_point': self._end_time_point}

    def set_status(self, is_success):
        self._last_status = JobStatus.STATUS_SUCCESS if is_success else JobStatus.STATUS_FAILURE

    def job_start(self, dt=None):
        if self._last_status == JobStatus.STATUS_SUCCESS:
            self._start_time_point = self._end_time_point
            if dt is None:
                dt = datetime.datetime.now().strftime(self._dfmt)
            self._end_time_point = dt
        self._last_status = JobStatus.STATUS_WORKING
        return self.get_ser_data()


def get_docs_for_processing(job_status):
    job_data = job_status.job_start()
    print 'working on %s' % job_data
    sql = sqldbutils.query_data(job_sql_template.format(**job_data))
    container = []
    sqldbutils.query_data(sql, container)
    return container


def do_copy_doc(src_doc_id, es, src_index, src_doc_type, dest_index, dest_doc_type):
    es.copy_doc(src_index, src_doc_type, src_doc_id, dest_index, dest_doc_type)


def working_on_docs(index_setting_file, job_file, src_index, src_doc_type, dest_index, dest_doc_type, num_threads=20):
    job_status = JobStatus(job_file)
    docs = get_docs_for_processing(job_status)
    es = ees.EntityCentricES.get_instance(index_setting_file)
    try:
        utils.multi_thread_tasking(docs, num_threads, do_copy_doc,
                                   args=[es, src_index, src_doc_type, dest_index, dest_doc_type])
        job_status.set_status(True)
    except:
        job_status.set_status(JobStatus.STATUS_FAILURE)
    job_status.save()



def copy_docs_by_patients(index_setting_file, src_index, src_doc_type, entity_id_field_name,
                          dest_index, dest_doc_type, doc_list_file):
    ees.copy_docs(index_setting_file, src_index, src_doc_type, entity_id_field_name,
                  dest_index, dest_doc_type, doc_list_file)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    index_setting_file = './index_settings/es_setting.json'
    src_index = ''
    src_doc_type = ''
    entity_id_field_name = ''
    dest_index = ''
    dest_doc_type = ''
    doc_list_file = ''
    # ees.copy_docs(index_setting_file, src_index, src_doc_type, entity_id_field_name,
    #               dest_index, dest_doc_type, doc_list_file)
    job_file = './job_status.json'
    working_on_docs(index_setting_file, job_file, src_index, src_doc_type, dest_index, dest_doc_type)
