import entity_centric_es as ees
import sys
import urllib3
from datetime import date, datetime
import datetime
import utils
from os.path import isfile, join
from os import listdir
import os
import sqldbutils
import xml.etree.ElementTree as ET
from subprocess import Popen, STDOUT
from entity_centric_es import EntityCentricES, do_index_100k_anns, do_index_100k_patients, JSONSerializerPython2
from elasticsearch import Elasticsearch
import cohortanalysis as cohort
import docanalysis
import logging


class ProcessSetting(object):
    def __init__(self, setting_file):
        self.__conf = None
        self.__file = setting_file
        self.load_data()

    def load_data(self):
        self.__conf = utils.load_json_data(self.__file)

    def get_attr(self, attr_path):
        dict_obj = self.__conf
        for e in attr_path:
            if e in dict_obj:
                dict_obj = dict_obj[e]
            else:
                return None
        return dict_obj


class JobStatus(object):
    """
    A JobStatus class for continuous processing on an incremental fashion
    e.g., doing updates every morning
    """
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


def get_docs_for_processing(job_status, job_sql_template, cnn_conf_file):
    """
    retrieve docs to process from a database table/view
    :param job_status:
    :return:
    """
    job_data = job_status.job_start()
    print 'working on %s' % job_data
    container = []
    sqldbutils.query_data(job_sql_template.format(**job_data), container,
                          dbconn=sqldbutils.get_db_connection_by_setting(cnn_conf_file))
    return container


def do_copy_doc(src_doc_id, es, src_index, src_doc_type, dest_index, dest_doc_type):
    """
    copy a cogstack doc from one index to the other
    :param src_doc_id:
    :param es:
    :param src_index:
    :param src_doc_type:
    :param dest_index:
    :param dest_doc_type:
    :return:
    """
    print 'copy %s ' % src_doc_id
    es.copy_doc(src_index, src_doc_type, str(src_doc_id), dest_index, dest_doc_type)


def working_on_docs(index_setting_file, job_file, src_index, src_doc_type, dest_index, dest_doc_type, num_threads=20):
    job_status = JobStatus(job_file)
    docs = get_docs_for_processing(job_status)
    print 'copy docs: [%s]' % docs
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
    """
    copy patient docs from one cogstack index to the other
    (used when different indices are used for different case studies)
    :param index_setting_file:
    :param src_index:
    :param src_doc_type:
    :param entity_id_field_name:
    :param dest_index:
    :param dest_doc_type:
    :param doc_list_file:
    :return:
    """
    ees.copy_docs(index_setting_file, src_index, src_doc_type, entity_id_field_name,
                  dest_index, dest_doc_type, doc_list_file)


def set_sys_env(settings):
    """
    set bash command environment
    :param settings:
    :return:
    """
    envs = settings.get_attr(['env'])
    for env in envs:
        os.environ[env.upper()] = envs[env]
    ukb_home = settings.get_attr(['env', 'ukb_home'])
    if ukb_home is not None and len(ukb_home) > 0 and ukb_home not in os.environ['PATH']:
        os.environ['PATH'] += ':' + ukb_home + '/bin'
    gate_home = settings.get_attr(['env', 'gate_home'])
    if gate_home is not None and len(gate_home) > 0 and gate_home not in os.environ['PATH']:
        os.environ['PATH'] += ':' + gate_home + '/bin'
    gcp_home = settings.get_attr(['env', 'gcp_home'])
    if gcp_home is not None and len(gcp_home) > 0 and gcp_home not in os.environ['PATH']:
        os.environ['PATH'] += ':' + gcp_home


def actionable_transparise(settings):
    cohort_name = settings.get_attr(['action_trans', 'cohort_name'])
    dbcnn_file = settings.get_attr(['action_trans', 'dbconn_setting_file'])
    sql_cohort_doc = settings.get_attr(['action_trans', 'sql_cohort_doc_template'])
    sql_doc_anns = settings.get_attr(['action_trans', 'sql_doc_anns_template'])
    sql_doc_content = settings.get_attr(['action_trans', 'sql_doc_content_template'])
    sql_action_trans_inert = settings.get_attr(['action_trans', 'sql_action_trans_update_template'])
    action_trans_model_file = settings.get_attr(['action_trans', 'action_trans_model_file'])
    cohort.action_transparentise(cohort_name, dbcnn_file,
                                 sql_cohort_doc, sql_doc_anns, sql_doc_content, sql_action_trans_inert,
                                 action_trans_model_file)


def produce_yodie_config(settings, data_rows, docid_path):
    """
    generate bio-yodie configuration xml file
    :param settings: the config instance
    :param data_rows: data arrays
    :param docid_path: the file path to save docids
    :return: number of docs to be processed
    """
    batch = ET.Element("batch")
    task_id = settings.get_attr(['job', 'job_id'])
    batch.set('id', 'semehr-%s' % task_id)
    batch.set('xmlns', "http://gate.ac.uk/ns/cloud/batch/1.0")

    application = ET.SubElement(batch, "application")
    application.set('file', '%s/bio-yodie-1-2-1/main-bio/main-bio.xgapp' % settings.get_attr(['env', 'yodie_path']))

    report = ET.SubElement(batch, "report")
    report_file = '%s/%s.xml' % (settings.get_attr(['env', 'yodie_path']), task_id)
    report.set('file', report_file)
    if settings.get_attr(['yodie', 'retain_report']) != 'yes':
        if os.path.isfile(report_file):
            os.unlink(report_file)

    input = ET.SubElement(batch, "input")
    input.set('encoding', 'UTF-8')
    num_docs = len(data_rows)
    if settings.get_attr(['yodie', 'input_source']) == "sql":
        input.set('class', 'kcl.iop.brc.core.kconnect.crisfeeder.CRISDocInputHandler')
        input.set('dbSettingImportant', 'true')
        input_db = utils.load_json_data(settings.get_attr(['yodie', 'input_dbconn_setting_file']))
        input.set('db_url', input_db['db_url'])
        input.set('db_driver', input_db['db_driver'])
        input.set('user', input_db['user'])
        input.set('password', input_db['password'])
        input.set('get_doc_sql_prefix', input_db['get_doc_sql_prefix'])
        logging.info('using docs from sql server [%s]' % settings.get_attr(['yodie', 'input_dbconn_setting_file']))
    elif settings.get_attr(['yodie', 'input_source']) == "files":
        dir_path = settings.get_attr(['yodie', 'input_doc_file_path'])
        num_docs = len([f for f in listdir(dir_path) if isfile(join(dir_path, f))])
        input.set('class', 'gate.cloud.io.file.FileInputHandler')
        input.set('dir', dir_path)
        documents = ET.SubElement(batch, "documents")
        documentEnumerator = ET.SubElement(documents, "documentEnumerator")
        documentEnumerator.set('class', 'gate.cloud.io.file.FileDocumentEnumerator')
        documentEnumerator.set('dir', settings.get_attr(['yodie', 'input_doc_file_path']))
        logging.info('using docs from folder [%s]' % dir_path)
    else:
        input.set('class', 'kcl.iop.brc.core.kconnect.crisfeeder.ESDocInputHandler')
        input.set('es_doc_url', '%s/%s/%s' % (
            settings.get_attr(['semehr', 'es_doc_url']), settings.get_attr(['semehr', 'full_text_index']),
            settings.get_attr(['semehr', 'full_text_doc_type'])))
        input.set('main_text_field', '%s' % settings.get_attr(['semehr', 'full_text_text_field']))
        input.set('doc_guid_field', '%s' % settings.get_attr(['semehr', 'full_text_doc_id']))
        input.set('doc_created_date_field', '%s' % settings.get_attr(['semehr', 'full_text_doc_date']))
        logging.info('using docs from elasticsearch [%s]' % settings.get_attr(['semehr', 'full_text_index']))

    output = ET.SubElement(batch, "output")
    if settings.get_attr(['yodie', 'output_destination']) == "sql":
        output.set('dbSettingImportant', 'true')
        output.set('class', 'kcl.iop.brc.core.kconnect.outputhandler.SQLOutputHandler')
        output_db = utils.load_json_data(settings.get_attr(['yodie', 'output_dbconn_setting_file']))
        output.set('db_url', output_db['db_url'])
        output.set('db_driver', output_db['db_driver'])
        output.set('user', output_db['user'])
        output.set('password', output_db['password'])
        output.set('output_table', '%s' % settings.get_attr(['yodie', 'output_table']))
        if settings.get_attr(['yodie', 'annotationOutputSettings']) is not None:
            output.set('annotationOutputSettings', settings.get_attr(['yodie', 'annotationOutputSettings']))
        if settings.get_attr(['yodie', 'docBasedOutput']) is not None:
            output.set('docBasedOutput', settings.get_attr(['yodie', 'docBasedOutput']))
        if settings.get_attr(['yodie', 'docAnnSQLTemplate']) is not None:
            output.set('docAnnSQLTemplate', settings.get_attr(['yodie', 'docAnnSQLTemplate']))
        if settings.get_attr(['yodie', 'singleAnnSQLTemplate']) is not None:
            output.set('singleAnnSQLTemplate', settings.get_attr(['yodie', 'singleAnnSQLTemplate']))
        if settings.get_attr(['yodie', 'output_concept_filter_file']) is not None:
            output.set('concept_filter', '%s' % settings.get_attr(['yodie', 'output_concept_filter_file']))
        logging.info('saving annotations to sql [%s]' % settings.get_attr(['yodie', 'output_dbconn_setting_file']))
    else:
        output.set('class', 'kcl.iop.brc.core.kconnect.outputhandler.YodieOutputHandler')
        output.set('output_folder', '%s' % settings.get_attr(['yodie', 'output_file_path']))
        output.set('file_based', '%s' % settings.get_attr(['yodie', 'use_file_based']))
        logging.info('saving annotations to folder [%s]' % settings.get_attr(['yodie', 'output_file_path']))

    if settings.get_attr(['yodie', 'input_source']) != "files":
        logging.info('doing yodie with %s documents, saved to %s...' %
                     (str(len(data_rows)), docid_path))
        # save doc ids to text file for input to bioyodie
        logging.info('saving doc ids to [%s]' % docid_path)
        utils.save_string('\n'.join([str(r['docid']) for r in data_rows]), docid_path)
        documents = ET.SubElement(batch, "documents")
        documentEnumerator = ET.SubElement(documents, "documentEnumerator")
        documentEnumerator.set('class', 'kcl.iop.brc.core.kconnect.crisfeeder.PlainTextEnumerator')
        documentEnumerator.set('doc_id_file', '%s/%s_docids.txt' % (
            settings.get_attr(['yodie', 'input_doc_file_path']), settings.get_attr(['job', 'job_id'])))

    tree = ET.ElementTree(batch)
    tree.write("%s" % settings.get_attr(['yodie', 'config_xml_path']), xml_declaration=True)
    return num_docs


def clear_folder(folder):
    """
    remove all files within a folder
    :param folder:
    :return:
    """
    if not os.path.exists(folder):
        return
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)


def do_semehr_index(settings, patients, doc_to_patient):
    """
    do SemEHR index
    :param settings:
    :param patients:
    :param doc_to_patient:
    :return:
    """
    es = EntityCentricES(settings.get_attr(['semehr', 'es_host']))
    es.index_name = settings.get_attr(['semehr', 'index'])
    es.concept_doc_type = settings.get_attr(['semehr', 'concept_doc_type'])
    es.entity_doc_type = settings.get_attr(['semehr', 'entity_doc_type'])
    es.doc_level_index = settings.get_attr(['semehr', 'doc_level_index'])

    f_yodie_anns = settings.get_attr(['yodie', 'output_file_path'])
    ann_files = [f for f in listdir(f_yodie_anns) if isfile(join(f_yodie_anns, f))]

    if settings.get_attr(['job', 'semehr-concept']) == 'yes':
        logging.info('[SemEHR-step] starting semehr-concept process')
        logging.debug('working on files : %s' % ann_files)
        # index concepts
        concept_index = settings.get_attr(['semehr', 'concept_index'])
        for ann in ann_files:
            utils.multi_thread_large_file_tasking(join(f_yodie_anns, ann), 10, do_index_100k_anns,
                                                  args=[es, doc_to_patient, concept_index])
        logging.info('[SemEHR-step-end]concept/document level indexing done')

    if settings.get_attr(['job', 'semehr-patients']) == 'yes':
        logging.info('[SemEHR-step] indexing annotations at patient level')
        # index patients
        es_doc_url = settings.get_attr(['semehr', 'es_doc_url'])
        es_full_text = Elasticsearch([es_doc_url], serializer=JSONSerializerPython2(), verify_certs=False)
        ft_index_name = settings.get_attr(['semehr', 'full_text_index'])
        ft_doc_type = settings.get_attr(['semehr', 'full_text_doc_type'])
        ft_entity_field = settings.get_attr(['semehr', 'full_text_patient_field'])
        ft_fulltext_field = settings.get_attr(['semehr', 'full_text_text_field'])
        utils.multi_thread_tasking(patients, 10, do_index_100k_patients,
                                   args=[es,
                                         es_full_text,
                                         ft_index_name,
                                         ft_doc_type,
                                         ft_entity_field,
                                         ft_fulltext_field])
        logging.info('[SemEHR-step-end]patient level indexing done')


def do_semehr_doc_anns_analysis(settings):
    anns_folder = settings.get_attr(['doc_ann_analysis', 'ann_docs_path'])
    text_folder = settings.get_attr(['doc_ann_analysis', 'full_text_folder'])
    full_text_file_pattern = settings.get_attr(['doc_ann_analysis', 'full_text_fn_ptn'])
    rule_config = settings.get_attr(['doc_ann_analysis', 'rule_config_path'])
    output_folder = settings.get_attr(['doc_ann_analysis', 'output_folder'])
    study_folder = settings.get_attr(['doc_ann_analysis', 'study_folder'])
    output_file_pattern = settings.get_attr(['doc_ann_analysis', 'output_fn_pattern'])
    thread_num = settings.get_attr(['doc_ann_analysis', 'thread_num'])
    if thread_num is None:
        thread_num = 10
    process_mode = settings.get_attr(['doc_ann_analysis', 'process_mode'])
    if process_mode is not None and process_mode != 'sql':
        docanalysis.process_doc_anns(anns_folder=anns_folder,
                                     full_text_folder=text_folder,
                                     rule_config_file=rule_config,
                                     output_folder=output_folder,
                                     study_folder=study_folder,
                                     full_text_fn_ptn=full_text_file_pattern,
                                     fn_pattern=output_file_pattern,
                                     thread_num=thread_num
                                     )
    else:
        ann_list_sql = settings.get_attr(['doc_ann_analysis', 'ann_list_sql'])
        primary_keys = settings.get_attr(['doc_ann_analysis', 'primary_keys'])
        ann_inst_sql = settings.get_attr(['doc_ann_analysis', 'ann_inst_sql'])
        full_text_sql = settings.get_attr(['doc_ann_analysis', 'full_text_sql'])
        update_query_template = settings.get_attr(['doc_ann_analysis', 'update_query_template'])
        update_status_template = settings.get_attr(['doc_ann_analysis', 'update_status_template'])
        dbconn_file = settings.get_attr(['doc_ann_analysis', 'dbconn_file'])
        docanalysis.analyse_db_doc_anns(ann_list_sql, ann_inst_sql, full_text_sql, primary_keys,
                                        update_query_template, dbconn_file,
                                        thread_num=thread_num,
                                        study_folder=study_folder,
                                        rule_config_file=rule_config,
                                        update_status_template=update_status_template
                                        )


def populate_cohort_results(settings):
    cohort_sql = settings.get_attr(['populate_cohort_result', 'cohort_sql'])
    doc_ann_sql_temp = settings.get_attr(['populate_cohort_result', 'doc_ann_sql_temp'])
    doc_ann_pks = settings.get_attr(['populate_cohort_result', 'doc_ann_pks'])
    dbcnn_file = settings.get_attr(['populate_cohort_result', 'dbconn_file'])
    study_folder = settings.get_attr(['populate_cohort_result', 'study_folder'])
    output_folder = settings.get_attr(['populate_cohort_result', 'output_folder'])
    sample_sql_temp = settings.get_attr(['populate_cohort_result', 'sample_sql_temp'])
    thread_num = settings.get_attr(['populate_cohort_result', 'thread_num'])
    sampling = settings.get_attr(['populate_cohort_result', 'sampling'])
    if sampling is None:
        sampling = True
    if sampling:
        sample_size = settings.get_attr(['populate_cohort_result', 'sample_size'])
    if sample_size is None:
        sample_size = 20
    docanalysis.db_populate_study_results(cohort_sql, doc_ann_sql_temp, doc_ann_pks, dbcnn_file,
                                          study_folder, output_folder, sample_sql_temp,
                                          thread_num=thread_num, sampling=sampling,
                                          sample_size=sample_size)


def process_semehr(config_file):
    """
    a pipeline to process all SemEHR related processes:
    0. ES doc copy from one index to another;
    1. bio-yodie NLP pipeline annotation on docs;
    2. entity centric SemEHR ES indexing
    :param config_file:
    :return:
    """
    # read the configuration
    ps = ProcessSetting(config_file)

    # setting log configuration
    log_level = 'INFO' if ps.get_attr(['logging', 'level']) is None else ps.get_attr(['logging', 'level'])
    log_format = '%(name)s %(asctime)s %(levelname)s %(message)s' if ps.get_attr(['logging', 'format']) is None \
        else ps.get_attr(['logging', 'format'])
    log_file = None if ps.get_attr(['logging', 'file']) is None else ps.get_attr(['logging', 'file'])
    logging.basicConfig(level=log_level, format=log_format)
    if log_file is not None:
        formatter = logging.Formatter(log_format)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
        logging.info('logging to %s' % log_file)

    # initialise the jobstatus class instance
    job_file = join(ps.get_attr(['job', 'job_status_file_path']),
                    'semehr_job_status_%s.json' % ps.get_attr(['job', 'job_id']))
    logging.info('[SemEHR-step] using job status file %s' % job_file)
    job_status = JobStatus(job_file)
    job_status.job_start()

    data_rows = []
    if ps.get_attr(['job', 'load_docs']) == 'yes':
        sql_template = ps.get_attr(['new_docs', 'sql_query'])
        logging.info('[SemEHR-step] retrieving docs by using the template [%s]' % sql_template)
        data_rows = get_docs_for_processing(job_status, sql_template, ps.get_attr(['new_docs', 'dbconn_setting_file']))
        logging.info('total docs num is %s' % len(data_rows))

    try:
    # if True:
        # 0. copy docs
        if ps.get_attr(['job', 'copy_docs']) == 'yes':
            logging.info('[SemEHR-step] copy docs')
            docs = [str(r['docid']) for r in data_rows]
            utils.multi_thread_tasking(docs, ps.get_attr(['doc_copy', 'thread_num']),
                                       do_copy_doc,
                                       args=[EntityCentricES(ps.get_attr(['doc_copy', 'es_host'])),
                                             ps.get_attr(['doc_copy', 'src_index']),
                                             ps.get_attr(['doc_copy', 'src_doc_type']),
                                             ps.get_attr(['doc_copy', 'dest_index']),
                                             ps.get_attr(['doc_copy', 'dest_doc_type'])])
            logging.info('[SemEHR-step-end]copying docs done')

        if ps.get_attr(['job', 'yodie']) == 'yes':
            docid_path = '%s/%s_docids.txt' % (
                ps.get_attr(['yodie', 'input_doc_file_path']), ps.get_attr(['job', 'job_id']))
            logging.info('[SemEHR-step] doing yodie')
            # 1. do bio-yodie pipeline
            # 1.1 prepare the configuration file
            num_docs = produce_yodie_config(ps, data_rows, docid_path)
            if num_docs == 0:
                logging.info('[SemEHR-step-end] nothing to process, NLP step done')
            else:
                logging.info('total number of docs %s' % num_docs)
                # 1.2 set the env variables
                set_sys_env(ps)
                # 1.3 clear ann output folder
                logging.info('clearing %s ...' % ps.get_attr(['yodie', 'output_file_path']))
                clear_folder(ps.get_attr(['yodie', 'output_file_path']))
                # 1.3 run bio-yodie
                os.chdir(ps.get_attr(['yodie', 'gcp_run_path']))
                if ps.get_attr(['yodie', 'os']) == 'win':
                    cmd = ' '.join(['java',
                                    "-Dgate.home=%s" % ps.get_attr(['env', 'gate_home']),
                                    "-Dgcp.home=%s" % ps.get_attr(['env', 'gcp_home']),
                                    "-Djava.protocol.handler.pkgs=gate.cloud.util.protocols",
                                    "-cp .;{SCRIPTDIR}/conf;{SCRIPTDIR}/gcp.jar;{SCRIPTDIR}/lib/*;"
                                    "{GATE_HOME}/bin/gate.jar;{GATE_HOME}/lib/*".format(
                                        **{"SCRIPTDIR":ps.get_attr(['env', 'gcp_home']),
                                           "GATE_HOME":ps.get_attr(['env', 'gate_home'])}),
                                    '-Dat.ofai.gate.modularpipelines.configFile="%s/bio-yodie-1-2-1/main-bio/main-bio.config.yaml" '
                                    % ps.get_attr(['env', 'yodie_path']),
                                    "-Xmx%s" % ps.get_attr(['yodie', 'memory']),
                                    "gate.cloud.batch.BatchRunner",
                                    "-t %s" % ps.get_attr(['yodie', 'thread_num']),
                                    "-b %s" % ps.get_attr(['yodie', 'config_xml_path'])
                                    ])
                else:
                    cmd = ' '.join(['gcp-direct.sh',
                                    "-t %s" % ps.get_attr(['yodie', 'thread_num']),
                                    "-Xmx%s" % ps.get_attr(['yodie', 'memory']),
                                    "-b %s" % ps.get_attr(['yodie', 'config_xml_path']),
                                    '-Dat.ofai.gate.modularpipelines.configFile="%s/bio-yodie-1-2-1/main-bio/main-bio.config.yaml" '
                                    % ps.get_attr(['env', 'yodie_path']),
                                    ])
                logging.debug('executing the following command to start NLP...')
                logging.info(cmd)
                p = Popen(cmd, shell=True, stderr=STDOUT)
                p.wait()

                if 0 != p.returncode:
                    job_status.set_status(False)
                    job_status.save()
                    logging.error('ERROR doing the NLP, stopped with a coide [%s]' % p.returncode)
                    exit(p.returncode)
                else:
                    logging.info('[SemEHR-step-end] NLP step done')
                if 'semehr_path' in os.environ:
                    logging.info('changing back to semehr_path: %s' % os.environ['semehr_path'])
                    os.chdir(os.environ['semehr_path'])

        # 2. do SemEHR concept/entity indexing
        if ps.get_attr(['job', 'semehr-concept']) == 'yes' or ps.get_attr(['job', 'semehr-patients']) == 'yes':
            patients = []
            doc_to_patient = {}
            for r in data_rows:
                patients.append(str(r['patientid']))
                doc_to_patient[str(r['docid'])] = str(r['patientid'])
            patients = list(set(patients))
            do_semehr_index(ps, patients, doc_to_patient)

        # 3. do SemEHR actionable transparency
        if ps.get_attr(['job', 'action_trans']) == 'yes':
            logging.info('[SemEHR-step]doing transparency...')
            actionable_transparise(settings=ps)

        # 4. do SemEHR document annotation analysis (post processing)
        if ps.get_attr(['job', 'doc_analysis']) == 'yes':
            logging.info('[SemEHR-step]doing SemEHR annotation analysis...')
            do_semehr_doc_anns_analysis(settings=ps)
            logging.info('[SemEHR-step-end] doc_analysis step done')

        # 5. do populate results for a research study
        if ps.get_attr(['job', 'populate_cohort_result']) == 'yes':
            logging.info('[SemEHR-step]doing SemEHR cohort result extraction...')
            populate_cohort_results(settings=ps)
            logging.info('[SemEHR-step-end] populate_cohort_result step done')

        job_status.set_status(True)
        job_status.save()
        logging.info('[SemEHR-process-end] all done')
    except Exception as e:
        logging.error('[SemEHR-process-ERROR] Failed to do SemEHR process %s' % str(e))
        job_status.set_status(False)
        job_status.save()


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('cp1252')
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    if len(sys.argv) != 2:
        print 'the syntax is [python semehr_processor.py PROCESS_SETTINGS_FILE_PATH]'
    else:
        process_semehr(sys.argv[1])


