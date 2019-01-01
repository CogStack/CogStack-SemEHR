import utils
from os.path import join, isfile, split
import logging
import study_analyzer
import sqldbutils as db
import json
import random
import re


class BasicAnn(object):
    """
    a simple NLP (Named Entity) annotation class
    """
    def __init__(self, str, start, end):
        self._str = str
        self._start = start
        self._end = end
        self._id = -1

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def str(self):
        return self._str

    @str.setter
    def str(self, value):
        self._str = value

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value):
        self._start = value

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, value):
        self._end = value

    def overlap(self, other_ann):
        if other_ann.start <= self.start <= other_ann.end or other_ann.start <= self.end <= other_ann.end:
            return True
        else:
            return False

    def is_larger(self, other_ann):
        return self.start <= other_ann.start and self.end >= other_ann.end \
               and not (self.start == other_ann.start and self.end == other_ann.end)

    def serialise_json(self):
        return {'start': self.start, 'end': self.end, 'str': self.str, 'id': self.id}

    @staticmethod
    def deserialise(jo):
        ann = BasicAnn(jo['start'], jo['start'], jo['end'])
        ann.id = jo['id']
        return ann


class ContextedAnn(BasicAnn):
    """
    a contextulised annotation class (negation/tempolarity/experiencer)
    """
    def __init__(self, str, start, end, negation, temporality, experiencer):
        self._neg = negation
        self._temp = temporality
        self._exp = experiencer
        super(ContextedAnn, self).__init__(str, start, end)

    @property
    def negation(self):
        return self._neg

    @negation.setter
    def negation(self, value):
        self._neg = value

    @property
    def temporality(self):
        return self._temp

    @temporality.setter
    def temporality(self, value):
        self._temp = value

    @property
    def experiencer(self):
        return self._exp

    @experiencer.setter
    def experiencer(self, value):
        self._exp = value

    def serialise_json(self):
        dict = super(ContextedAnn, self).serialise_json()
        dict['negation'] = self.negation
        dict['temporality'] = self.temporality
        dict['experiencer'] = self.experiencer
        return dict


class PhenotypeAnn(ContextedAnn):
    """
    a simple customisable phenotype annotation (two attributes for customised attributes)
    """
    def __init__(self, str, start, end,
                 negation, temporality, experiencer,
                 major_type, minor_type):
        super(PhenotypeAnn, self).__init__(str, start, end, negation, temporality, experiencer)
        self._major_type = major_type
        self._minor_type = minor_type

    @property
    def major_type(self):
        return self._major_type

    @major_type.setter
    def major_type(self, value):
        self._major_type = value

    @property
    def minor_type(self):
        return self._minor_type

    @minor_type.setter
    def minor_type(self, value):
        self._minor_type = value

    def serialise_json(self):
        dict = super(PhenotypeAnn, self).serialise_json()
        dict['major_type'] = self.major_type
        dict['minor_type'] = self.minor_type
        return dict

    @staticmethod
    def deserialise(jo):
        ann = PhenotypeAnn(jo['str'], jo['start'], jo['end'], jo['negation'], jo['temporality'],
                           jo['experiencer'], jo['major_type'], jo['minor_type'])
        ann.id = jo['id']
        return ann


class SemEHRAnn(ContextedAnn):
    """
    SemEHR Annotation Class
    """
    def __init__(self, str, start, end,
                 negation, temporality, experiencer,
                 cui, sty, pref, ann_type):
        super(SemEHRAnn, self).__init__(str, start, end, negation, temporality, experiencer)
        self._cui = cui
        self._sty = sty
        self._pref = pref
        self._ann_type = ann_type
        self._study_concepts = []
        self._ruled_by = []

    @property
    def cui(self):
        return self._cui

    @cui.setter
    def cui(self, value):
        self._cui = value

    @property
    def sty(self):
        return self._sty

    @sty.setter
    def sty(self, value):
        self._sty = value

    @property
    def ann_type(self):
        return self._ann_type

    @ann_type.setter
    def ann_type(self, value):
        self._ann_type = value

    @property
    def pref(self):
        return self._pref

    @pref.setter
    def pref(self, value):
        self._pref = value

    @property
    def study_concepts(self):
        return self._study_concepts

    def add_study_concept(self, value):
        if value not in self._study_concepts:
            self._study_concepts.append(value)

    @property
    def ruled_by(self):
        return self._ruled_by

    def add_ruled_by(self, rule_name):
        if rule_name not in self._ruled_by:
            self._ruled_by.append(rule_name)

    def serialise_json(self):
        dict = super(SemEHRAnn, self).serialise_json()
        dict['sty'] = self.sty
        dict['cui'] = self.cui
        dict['pref'] = self.pref
        dict['study_concepts'] = self.study_concepts
        dict['ruled_by'] = self.ruled_by
        return dict

    @staticmethod
    def deserialise(jo):
        ann = SemEHRAnn(jo['str'], jo['start'], jo['end'], jo['negation'], jo['temporality'],
                        jo['experiencer'], jo['cui'], jo['sty'], jo['pref'], 'mention')
        ann.id = jo['id']
        if 'ruled_by' in jo:
            ann._ruled_by = jo['ruled_by']
        if 'study_concepts' in jo:
            ann._ruled_by = jo['study_concepts']
        return ann


class SemEHRAnnDoc(object):
    """
    SemEHR annotation Doc
    """
    def __init__(self, file_key=None):
        self._fk = file_key
        self._anns = []
        self._phenotype_anns = []
        self._sentences = []
        self._others = []

    def load(self, json_doc, file_key=None):
        self._doc = json_doc
        if file_key is not None:
            self._fk = file_key
        self.load_anns()

    def load_anns(self):
        all_anns = self._anns
        panns = self._phenotype_anns
        if 'sentences' in self._doc:
            # is a SemEHRAnnDoc serialisation
            self._anns = [SemEHRAnn.deserialise(a) for a in self._doc['annotations']]
            if 'phenotypes' in self._doc:
                self._phenotype_anns = [PhenotypeAnn.deserialise(a) for a in self._doc['phenotypes']]
            self._sentences = [BasicAnn.deserialise(a) for a in self._doc['sentences']]
        else:
            for anns in self._doc['annotations']:
                for ann in anns:
                    t = ann['type']
                    if t == 'Mention':
                        a = SemEHRAnn(ann['features']['string_orig'],
                                      int(ann['startNode']['offset']),
                                      int(ann['endNode']['offset']),

                                      ann['features']['Negation'],
                                      ann['features']['Temporality'],
                                      ann['features']['Experiencer'],

                                      ann['features']['inst'],
                                      ann['features']['STY'],
                                      ann['features']['PREF'],
                                      t)
                        all_anns.append(a)
                        a.id = 'cui-%s' % len(all_anns)
                    elif t == 'Phenotype':
                        a = PhenotypeAnn(ann['features']['string_orig'],
                                         int(ann['startNode']['offset']),
                                         int(ann['endNode']['offset']),

                                         ann['features']['Negation'],
                                         ann['features']['Temporality'],
                                         ann['features']['Experiencer'],

                                         ann['features']['majorType'],
                                         ann['features']['minorType'])
                        panns.append(a)
                        a.id = 'phe-%s' % len(panns)
                    elif t == 'Sentence':
                        a = BasicAnn('Sentence',
                                     int(ann['startNode']['offset']),
                                     int(ann['endNode']['offset']))
                        self._sentences.append(a)
                        a.id = 'sent-%s' % len(self._sentences)
                    else:
                        self._others.append(ann)

            sorted(all_anns, key=lambda x: x.start)

    @property
    def file_key(self):
        return self._fk

    def get_ann_sentence(self, ann):
        sent = None
        for s in self.sentences:
            if ann.overlap(s):
                sent = s
                break
        if sent is None:
            print 'sentence not found for %s' % ann.__dict__
            return None
        return sent

    @property
    def annotations(self):
        return self._anns

    @property
    def sentences(self):
        return self._sentences

    @property
    def phenotypes(self):
        return self._phenotype_anns

    def serialise_json(self):
        return {'annotations': [ann.serialise_json() for ann in self.annotations],
                'phenotypes': [ann.serialise_json() for ann in self.phenotypes],
                'sentences': [ann.serialise_json() for ann in self.sentences]}


class FulltextReader(object):

    def __init__(self, folder, pattern):
        self._folder = folder
        self._pattern = pattern

    def read_full_text(self, fk):
        p = join(self._folder, self._pattern % fk)
        if isfile(p):
            return utils.read_text_file_as_string(p)
        else:
            return None


def process_doc_rule(ann_doc, rule_executor, text, study_analyzer):
    study_concepts = study_analyzer.study_concepts if study_analyzer is not None else None
    num_concepts = 0
    for ann in ann_doc.annotations:
        is_a_concept = False
        if study_concepts is not None:
            for sc in study_concepts:
                if ann.cui in sc.concept_closure:
                    ann.add_study_concept(sc.name)
                    is_a_concept = True
                    logging.debug('%s [%s, %s] is one %s' % (ann.str, ann.start, ann.end, sc.name))
        else:
            is_a_concept = True
        if is_a_concept:
            sent = ann_doc.get_ann_sentence(ann)
            if sent is not None:
                ruled = False
                context_text = text[sent.start:sent.end]
                s_before = context_text[:ann.start-sent.start]
                s_end = context_text[ann.end-sent.start:]
                if not ruled:
                    # string orign rules - not used now
                    ruled, case_instance = rule_executor.execute_original_string_rules(ann.str)
                    rule = 'original-string-rule'
                if not ruled:
                    # post processing rules
                    ruled, case_instance, rule = \
                        rule_executor.execute_context_text(text, s_before, s_end, ann.str)
                if ruled:
                    ann.add_ruled_by(rule)
                    logging.debug('%s [%s, %s] ruled by %s' % (ann.str, ann.start, ann.end, rule))
            num_concepts += 1
    return num_concepts


def db_doc_process(row, sql_template, pks, update_template, dbcnn_file, sa, ruler):
    sql = sql_template.format(*[row[k] for k in pks])
    logging.debug('query ann: %s' % sql)
    rets = []
    db.query_data(sql, rets, db.get_db_connection_by_setting(dbcnn_file))
    if len(rets) > 0:
        anns = json.loads(rets[0]['anns'])
        ann_doc = SemEHRAnnDoc()
        ann_doc.load(anns)
        if len(ann_doc.annotations) > 0:
            text = rets[0]['text']
            num_concepts = process_doc_rule(ann_doc, ruler, text, sa)
            if num_concepts > 0:
                update_query = update_template.format(*([db.escape_string(json.dumps(ann_doc.serialise_json()))] +
                                                        [row[k] for k in pks]))
                # logging.debug('update ann: %s' % update_query)
                db.query_data(update_query, None, db.get_db_connection_by_setting(dbcnn_file))
                logging.info('ann %s updated' % row)
            else:
                logging.info('no concepts found/update %s' % row)
        else:
            logging.debug('ann skipped, %s annotation empty' % row)


def analyse_db_doc_anns(sql, ann_sql, pks, update_template, dbcnn_file, rule_config_file,
                        study_folder, thread_num=10, study_config='study.json'):
    """
    do database based annotation post processing
    :param sql: get a list of annotation primary keys
    :param ann_sql: a query template to query ann and its doc full text
    :param pks: an array of primary key columns
    :param update_template: an update query template to update post-processed ann
    :param dbcnn_file: database connection file
    :param thread_num:
    :param study_folder:
    :param rule_config_file:
    :param study_config:
    :return:
    """
    ret = load_study_ruler(study_folder, rule_config_file, study_config)
    sa = ret['sa']
    ruler = ret['ruler']
    rows = []
    db.query_data(sql, rows, db.get_db_connection_by_setting(dbcnn_file))
    utils.multi_thread_tasking(rows, thread_num, db_doc_process,
                               args=[ann_sql, pks, update_template, dbcnn_file, sa, ruler])


def analyse_doc_anns(ann_doc_path, rule_executor, text_reader, output_folder, fn_pattern='se_ann_%s.json',
                     study_analyzer=None):
    p, fn = split(ann_doc_path)
    file_key = fn[:fn.index('.')]
    json_doc = utils.load_json_data(ann_doc_path)
    ann_doc = SemEHRAnnDoc()
    ann_doc.load(json_doc, file_key=file_key)
    text = text_reader.read_full_text(ann_doc.file_key)
    if text is None:
        logging.error('file [%s] full text not found' % ann_doc.file_key)
        return

    process_doc_rule(ann_doc, rule_executor, text, study_analyzer)
    utils.save_json_array(ann_doc.serialise_json(), join(output_folder, fn_pattern % ann_doc.file_key))
    return ann_doc.serialise_json()


def load_study_ruler(study_folder, rule_config_file, study_config='study.json'):
    if study_folder is not None and study_folder != '':
        r = utils.load_json_data(join(study_folder, study_config))

        ret = study_analyzer.load_study_settings(study_folder,
                                                 umls_instance=None,
                                                 rule_setting_file=r['rule_setting_file'],
                                                 concept_filter_file=None if 'concept_filter_file' not in r else r['concept_filter_file'],
                                                 do_disjoint_computing=True if 'do_disjoint' not in r else r['do_disjoint'],
                                                 export_study_concept_only=False if 'export_study_concept' not in r else r['export_study_concept']
                                                 )
        sa = ret['study_analyzer']
        ruler = ret['ruler']
    else:
        logging.info('no study configuration provided, applying rules to all annotations...')
        ruler = study_analyzer.load_ruler(rule_config_file)
    return {'sa': sa, 'ruler': ruler}


def process_doc_anns(anns_folder, full_text_folder, rule_config_file, output_folder,
                     study_folder=None,
                     study_config='study.json', full_text_fn_ptn='%s.txt', fn_pattern='se_ann_%s.json',
                     thread_num=10):
    """
    multiple threading process doc anns
    :param anns_folder:
    :param full_text_folder:
    :param rule_config_file:
    :param output_folder:
    :param study_folder:
    :param study_config:
    :param full_text_fn_ptn:
    :param fn_pattern:
    :return:
    """
    text_reader = FulltextReader(full_text_folder, full_text_fn_ptn)
    ret = load_study_ruler(study_folder, rule_config_file, study_config)
    sa = ret['sa']
    ruler = ret['ruler']

    # for ff in [f for f in listdir(anns_folder) if isfile(join(anns_folder, f))]:
    #     analyse_doc_anns(join(anns_folder, ff), ruler, text_reader, output_folder, fn_pattern, sa)
    utils.multi_thread_process_files(dir_path=anns_folder,
                                     file_extension='json',
                                     num_threads=thread_num,
                                     process_func=analyse_doc_anns,
                                     args=[ruler, text_reader, output_folder, fn_pattern, sa])
    logging.info('post processing of ann docs done')


def db_populate_patient_result(pid, doc_ann_sql_temp, doc_ann_pks, dbcnn_file, concept_list, container, study_concepts):
    """
    populate a row (per patient) in the result table
    :param pid:
    :param doc_ann_sql_temp:
    :param doc_ann_pks:
    :param dbcnn_file:
    :param concept_list:
    :param container:
    :return:
    """
    rows = []
    db.query_data(doc_ann_sql_temp.format(pid), rows, db.get_db_connection_by_setting(dbcnn_file))
    c2f = {}
    for c in concept_list:
        c2f[c] = {'f': 0, 'rf': 0, 'docs': []}
    logging.debug('pid: %s has %s docs' % (pid, len(rows)))
    i = 0
    for r in rows:
        try:
            i += 1
            logging.debug('working on doc #%s' % i)
            anns = json.loads(fix_escaped_issue(r['anns']))
            ann_doc = SemEHRAnnDoc()
            ann_doc.load(anns)
            for a in ann_doc.annotations:
                # for c in a.study_concepts:
                is_concept = False
                sc_name = None
                for sc in study_concepts:
                    if a.cui in sc.concept_closure:
                        is_concept = True
                        sc_name = sc.name
                if is_concept:
                    logging.debug('%s found in %s, ruled_by=%s' % (sc_name, r['doc_id'], a.ruled_by))
                    if c in c2f:
                        if len(a.ruled_by) > 0:
                            c2f[c]['rf'] += 1
                        else:
                            c2f[c]['f'] += 1
                            c2f[c]['docs'].append([r[k] for k in doc_ann_pks])
        except Exception as e:
            logging.error('parsing anns %s because of %s' % (fix_escaped_issue(r['anns']), str(e)))
    logging.info('pid %s done' % pid)
    container.append({'p': pid, 'c2f': c2f})


def fix_escaped_issue(s):
    return re.sub(
        r'(string_orig":"|pref": "|PREF":"|str": ")(((?!","|"\}|", ").)*)(","|"\}|", ")',
        r'\1\4',
        s
    )


def extract_sample(pk_vals, concept, sample_sql_temp, dbcnn_file, container):
    """
    extract an sample
    :param pk_vals:
    :param concept:
    :param sample_sql_temp:
    :param dbcnn_file:
    :param container:
    :return:
    """
    rows = []
    db.query_data(sample_sql_temp.format(*[v for v in pk_vals]), rows,
                  db.get_db_connection_by_setting(dbcnn_file))
    if len(rows) > 0:
        r = rows[0]
        anns = json.loads(r['anns'])
        ann_doc = SemEHRAnnDoc()
        ann_doc.load(anns)
        for a in ann_doc.annotations:
            if concept in a.study_concepts:
                container.append({'content': r['text'], 'doc_table': r['src_table'], 'doc_col': r['src_col'],
                                  'id': '_'.join(pk_vals),
                                  'annotations': [{'start': a.start,
                                                   'end': a.end,
                                                   'concept': a.cui,
                                                   'string_orig': a.str}]})
                break


def db_populate_study_results(cohort_sql, doc_ann_sql_temp, doc_ann_pks, dbcnn_file,
                              study_folder, output_folder, sample_sql_temp,
                              thread_num=10, study_config='study.json',
                              sampling=True, sample_size=20):
    """
    populate results for a research study
    :param cohort_sql: cohort selection query
    :param doc_ann_sql_temp: query template for getting a doc_anns item
    :param doc_ann_pks: primary key columns of doc ann table
    :param dbcnn_file: database connection config file
    :param study_folder: study folder
    :param output_folder: where to save the results
    :param sample_sql_temp: query template for getting a sample item (including full text and doc_anns)
    :param thread_num:
    :param study_config:
    :param sampling: whether sampling is needed
    :param sample_size: how many samples per study concept
    :return:
    """
    ret = load_study_ruler(study_folder, None, study_config)
    sa = ret['sa']
    concept_list = sorted([sc.name for sc in sa.study_concepts])
    results = []
    rows = []
    db.query_data(cohort_sql, rows, db.get_db_connection_by_setting(dbcnn_file))
    logging.info('querying results (cohort size:%s)...' % len(rows))
    utils.multi_thread_tasking([r['pid'] for r in rows], thread_num, db_populate_patient_result,
                               args=[doc_ann_sql_temp, doc_ann_pks, dbcnn_file, concept_list, results, sa.study_concepts])
    # populate result table
    c2pks = {}
    for c in concept_list:
        c2pks[c] = []
    s = '\t'.join(['pid'] + concept_list)
    for r in results:
        pr = [r['p']]
        for c in concept_list:
            if r['c2f'][c]['f'] > 0:
                c2pks[c].append(r['c2f'][c]['docs'][0])
            pr.append(str(r['c2f'][c]['f']))
        s += '\t'.join(pr) + '\n'
    f = join(output_folder, 'result.tsv')
    utils.save_string(s, f)
    logging.info('result table saved to [%s]' % f)
    if sampling:
        logging.info('doing sampling...')
        sampled_result = {}
        for c in c2pks:
            pks = c2pks[c]
            sample_pks = []
            if len(pks) <= sample_size:
                sample_pks = pks
            else:
                for i in xrange(sample_size):
                    index = random.randrange(len(pks))
                    sample_pks.append(pks[index])
                    del pks[index]
            samples = []
            utils.multi_thread_tasking(sample_pks, thread_num, extract_sample,
                                       args=[c, sample_sql_temp, dbcnn_file, samples])
            sampled_result[c] = samples
            logging.info('%s sampled (%s) results' % (c, len(samples)))

        f = join(output_folder, 'sampled_docs.js')
        utils.save_string('var sample_docs= %s;' % json.dumps(sampled_result), f)
        logging.info('samples saved to %s' % f)
    logging.info('all results populated')


if __name__ == "__main__":
    pass
