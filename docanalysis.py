import utils
from os.path import join, isfile, split
import logging
import study_analyzer
import sqldbutils as db
import json
import random
import re
import ann_post_rules
import multiprocessing


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

    @study_concepts.setter
    def study_concepts(self, value):
        self._study_concepts = value

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
            ann._study_concepts = jo['study_concepts']
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
                        self._sentences = sorted(self._sentences, key=lambda x:x.start)
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

    def get_prev_sent(self, s):
        self._sentences = sorted(self._sentences, key=lambda x:x.start)
        for idx in xrange(len(self.sentences)):
            if self.sentences[idx] == s:
                if idx > 0:
                    return self.sentences[idx-1]
                else:
                    return None

    def get_next_sent(self, s):
        self._sentences = sorted(self._sentences, key=lambda x:x.start)
        for idx in xrange(len(self.sentences)):
            if self.sentences[idx] == s:
                if idx < len(self.sentences) - 1:
                    return self.sentences[idx+1]
                else:
                    return None

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


class TextReader(object):
    def __init__(self):
        pass

    def read_full_text(self, text_key):
        pass


class FileTextReader(TextReader):

    def __init__(self, folder, pattern):
        self._folder = folder
        self._pattern = pattern

    def read_full_text(self, fk):
        p = join(self._folder, self._pattern % fk)
        if isfile(p):
            return utils.read_text_file_as_string(p)
        else:
            return None


class WrapperTextReader(TextReader):
    def __init__(self, text):
        self._text = text

    def read_full_text(self, text_key):
        return self._text


class DBTextReader(TextReader):
    def __init__(self, sql_temp, dbcnn_file):
        self._qt = sql_temp
        self._cnn_file = dbcnn_file

    def read_full_text(self, text_key):
        sql = self._qt.format(*[k for k in text_key])
        rets = []
        db.query_data(sql, rets, db.get_db_connection_by_setting(self._cnn_file))
        return rets[0]['text']


class ESTextReader(TextReader):
    def __init__(self, es, full_text_field):
        self._es = es
        self._text_field = full_text_field

    def read_full_text(self, text_key):
        doc = self._es.get_doc_detail(text_key)
        if doc is not None:
            return doc[self._text_field]
        else:
            return None


def process_doc_rule(ann_doc, rule_executor, reader, text_key, study_analyzer, reset_prev_concept=False):
    study_concepts = study_analyzer.study_concepts if study_analyzer is not None else None
    num_concepts = 0
    text = None
    for ann in ann_doc.annotations:
        is_a_concept = False
        if reset_prev_concept:
            ann.study_concepts = []
        if study_concepts is not None:
            for sc in study_concepts:
                if ann.cui in sc.concept_closure:
                    ann.add_study_concept(sc.name)
                    is_a_concept = True
                    logging.info('%s [%s, %s] is one %s' % (ann.str, ann.start, ann.end, sc.name))
        else:
            is_a_concept = True
        if is_a_concept:
            # lazy reading to ignore unnecessary full text reading
            if text is None:
                text = reader.read_full_text(text_key).replace('\n', ' ')
            sent = ann_doc.get_ann_sentence(ann)
            if sent is not None:
                ruled = False
                context_text = text[sent.start:sent.end]
                offset_start = ann.start - sent.start
                offset_end = ann.end - sent.start
                offset = 0
                anchor_sent = sent
                if context_text[offset_start:offset_end].lower() != ann.str.lower():
                    [s, e] = ann_post_rules.AnnRuleExecutor.relocate_annotation_pos(text,
                                                                                    ann.start, ann.end, ann.str)
                    offset = s - ann.start
                    logging.debug('offset not matching, relocated from %s,%s to %s,%s, offset: %s' %
                                  (ann.start, ann.end, s, e, offset))
                    context_text = text[sent.start + offset:sent.end+offset]
                    logging.debug('context text: %s' % context_text)
                s_before = context_text[:offset_start]
                if context_text.startswith('s ') or s_before == '' :
                    prev_s = ann_doc.get_prev_sent(sent)
                    if prev_s is not None:
                        s_before = text[prev_s.start + offset:prev_s.end + offset] + s_before
                        anchor_sent = prev_s
                    else:
                        logging.debug('previous sentence not found %s' % sent.id)
                s_end = context_text[offset_end:]
                if context_text.endswith('?'):
                    next_s = ann_doc.get_next_sent(sent)
                    if next_s is not None:
                        s_end = s_end + text[next_s.start + offset:next_s.end + offset]
                        anchor_sent = next_s
                more_context_sents = {}
                prev_s = ann_doc.get_prev_sent(anchor_sent)
                if prev_s is not None:
                    more_context_sents['prev'] = text[prev_s.start + offset:prev_s.end + offset]
                next_s = ann_doc.get_next_sent(anchor_sent)
                if next_s is not None:
                    more_context_sents['next'] = text[next_s.start + offset:next_s.end + offset]

                str_orig = ann.str if context_text[offset_start:offset_end].lower() != ann.str.lower() else \
                    context_text[offset_start:offset_end]
                # logging.debug('%s' % context_text)
                logging.debug('[%s] <%s> [%s]' % (s_before, str_orig, s_end))
                if not ruled:
                    # string orign rules - not used now
                    ruled, case_instance = rule_executor.execute_original_string_rules(str_orig)
                    rule = 'original-string-rule'
                if not ruled:
                    # post processing rules
                    ruled, case_instance, rule = \
                        rule_executor.execute_context_text(text, s_before, s_end, str_orig,
                                                           more_context_sents=more_context_sents)
                if ruled:
                    ann.add_ruled_by(rule)
                    logging.info('%s [%s, %s] ruled by %s' % (str_orig, ann.start, ann.end, rule))
            else:
                logging.error('sentence not found for ann %s,%s %s' % (ann.start, ann.end, ann.str))
            num_concepts += 1
    return num_concepts


def db_doc_process(row, sql_template, pks, update_template, dbcnn_file, text_reader, sa, ruler, update_status_template):
    sql = sql_template.format(*[row[k] for k in pks])
    rets = []
    db.query_data(sql, rets, db.get_db_connection_by_setting(dbcnn_file))
    if len(rets) > 0:
        anns = json.loads(fix_escaped_issue(rets[0]['anns']))
        ann_doc = SemEHRAnnDoc()
        ann_doc.load(anns)
        no_concepts = False
        if len(ann_doc.annotations) > 0:
            num_concepts = process_doc_rule(ann_doc, ruler, text_reader, [row[k] for k in pks], sa)
            if num_concepts > 0:
                update_query = update_template.format(*([db.escape_string(json.dumps(ann_doc.serialise_json()))] +
                                                        [row[k] for k in pks]))
                # logging.debug('update ann: %s' % update_query)
                db.query_data(update_query, None, db.get_db_connection_by_setting(dbcnn_file))
                logging.info('ann %s updated' % row)
            else:
                no_concepts = True
        else:
            no_concepts = True
        if no_concepts and update_status_template is not None:
            q = update_status_template.format(*[row[k] for k in pks])
            db.query_data(q, None, db.get_db_connection_by_setting(dbcnn_file))
            logging.debug('no concepts found/update %s' % q)


def analyse_db_doc_anns(sql, ann_sql, pks, update_template, full_text_sql, dbcnn_file, rule_config_file,
                        study_folder, thread_num=10, study_config='study.json', update_status_template=None):
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
    reader = DBTextReader(full_text_sql, dbcnn_file)
    utils.multi_process_tasking(rows, db_doc_process, num_procs=thread_num,
                                args=[ann_sql, pks, update_template, dbcnn_file, reader, sa, ruler,
                                      update_status_template])


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
    reader = WrapperTextReader(text)
    process_doc_rule(ann_doc, rule_executor, reader, None, study_analyzer)
    utils.save_json_array(ann_doc.serialise_json(), join(output_folder, fn_pattern % ann_doc.file_key))
    return ann_doc.serialise_json()


def load_study_ruler(study_folder, rule_config_file, study_config='study.json'):
    sa = None
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
                     thread_num=10, es_inst=None, es_text_field=''):
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
    :param thread_num:
    :param es_inst: semquery.SemEHRES instance
    :param es_text_field: the full text filed name in the es index
    :return:
    """
    if es_inst is None:
        text_reader = FileTextReader(full_text_folder, full_text_fn_ptn)
    else:
        text_reader = ESTextReader(es_inst, es_text_field)
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


def db_populate_patient_result(container, pid, doc_ann_sql_temp, doc_ann_pks, dbcnn_file, concept_list,
                               ontext_filter_fun=None):
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
            anns = json.loads(fix_escaped_issue(r['anns']))
            ann_doc = SemEHRAnnDoc()
            ann_doc.load(anns)
            for a in ann_doc.annotations:
                for c in a.study_concepts:
                    logging.debug('%s found in %s, ruled_by=%s, concepts:%s' % (c, '-'.join([r[k] for k in doc_ann_pks]),
                                                                   a.ruled_by, a.study_concepts))
                    if c in c2f:
                        correct = len(a.ruled_by) == 0
                        if correct and ontext_filter_fun is not None:
                            correct = ontext_filter_fun(a)
                        if not correct:
                            c2f[c]['rf'] += 1
                        else:
                            c2f[c]['f'] += 1
                            c2f[c]['docs'].append([r[k] for k in doc_ann_pks])
        except Exception as e:
            logging.error('parsing anns %s because of %s' % (fix_escaped_issue(r['anns']), str(e)))
    logging.info('pid %s done' % pid)
    container.append({'p': pid, 'c2f': c2f})
    logging.debug('pid %s with %s, %s' % (pid, len(c2f), len(container)))


def positive_patient_filter(ann):
    return ann.negation == 'Affirmed' and ann.experiencer == 'Patient'


def fix_escaped_issue(s):
    p = re.compile(r'(string_orig":"|pref": "|PREF":"|str": ")(((?!","|"\}|", ").)*)(","|"\}|", ")')
    fiz = []
    for m in p.finditer(s):
        vg = 2
        val = s[m.span(vg)[0]:m.span(vg)[1]]
        if '"' in val:
            new_val = val.replace('\\', '').replace('"', '\\"')
            fix = {'s': m.span()[0], 'e': m.span()[1], 'mid': s[m.span(1)[0]:m.span(1)[1]] + new_val + s[m.span(4)[0]:m.span(4)[1]]}
            fiz.append(fix)
    new_s = s
    if len(fiz) > 0:
        logging.debug('fixes needed: %s' % fiz)
        new_s = ''
        last_pos = 0
        for f in fiz:
            new_s += '%s%s' % (s[last_pos:f['s']], f['mid'])
            last_pos = f['e']
        new_s += s[last_pos:]
    return new_s


def extract_sample(pk_vals, concept, sample_sql_temp, dbcnn_file, container, ontext_filter_fun=positive_patient_filter):
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
                correct = len(a.ruled_by) == 0
                if correct and ontext_filter_fun is not None:
                    correct = ontext_filter_fun(a)
                if correct:
                    container.append({'content': r['text'], 'doc_table': r['src_table'], 'doc_col': r['src_col'],
                                      'id': '_'.join(pk_vals),
                                      'annotations': [{'start': a.start,
                                                       'end': a.end,
                                                       'concept': a.cui,
                                                       'string_orig': a.str}]})
                    break


def proc_init_container():
    manager = multiprocessing.Manager()
    return manager.list()


def proc_final_collect(container, results):
    logging.debug('collecting %s' % len(container))
    for r in container:
        results.append(r)


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
    utils.multi_process_tasking([r['pid'] for r in rows], db_populate_patient_result, num_procs=thread_num,
                                args=[doc_ann_sql_temp, doc_ann_pks, dbcnn_file, concept_list,
                                      positive_patient_filter],
                                thread_init_func=proc_init_container,
                                thread_end_func=proc_final_collect,
                                thread_end_args=[results]
                                )
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


def init_study_config(study_folder):
    load_study_ruler(study_folder, None)


if __name__ == "__main__":
    init_study_config('./studies/autoimmune.v3')
