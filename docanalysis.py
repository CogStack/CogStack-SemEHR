import utils
from os.path import join, isfile, split, splitext
from os import listdir
import logging
import study_analyzer
import json
import re
import ann_post_rules
import multiprocessing


class BasicAnn(object):
    """
    a simple NLP (Named Entity) annotation class
    """

    def __init__(self, str_, start, end):
        self._str = str_
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

    def __init__(self, str_, start, end, negation, temporality, experiencer):
        self._neg = negation
        self._temp = temporality
        self._exp = experiencer
        super(ContextedAnn, self).__init__(str_, start, end)

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
        dict_obj = super(ContextedAnn, self).serialise_json()
        dict_obj['negation'] = self.negation
        dict_obj['temporality'] = self.temporality
        dict_obj['experiencer'] = self.experiencer
        return dict_obj


class PhenotypeAnn(ContextedAnn):
    """
    a simple customisable phenotype annotation (two attributes for customised attributes)
    """

    def __init__(self, str_, start, end,
                 negation, temporality, experiencer,
                 major_type, minor_type):
        super(PhenotypeAnn, self).__init__(str_, start, end, negation, temporality, experiencer)
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
        dict_ojb = super(PhenotypeAnn, self).serialise_json()
        dict_ojb['major_type'] = self.major_type
        dict_ojb['minor_type'] = self.minor_type
        return dict_ojb

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

    def __init__(self, str_, start, end,
                 negation, temporality, experiencer,
                 cui, sty, pref, ann_type):
        super(SemEHRAnn, self).__init__(str_, start, end, negation, temporality, experiencer)
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
        dict_obj = super(SemEHRAnn, self).serialise_json()
        dict_obj['sty'] = self.sty
        dict_obj['cui'] = self.cui
        dict_obj['pref'] = self.pref
        dict_obj['study_concepts'] = self.study_concepts
        dict_obj['ruled_by'] = self.ruled_by
        return dict_obj

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
        self._doc = None

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
                        self._sentences = sorted(self._sentences, key=lambda x: x.start)
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
            print('sentence not found for %s' % ann.__dict__)
            return None
        return sent

    def get_prev_sent(self, s):
        self._sentences = sorted(self._sentences, key=lambda x: x.start)
        for idx in range(len(self.sentences)):
            if self.sentences[idx] == s:
                if idx > 0:
                    return self.sentences[idx - 1]
                else:
                    return None

    def get_next_sent(self, s):
        self._sentences = sorted(self._sentences, key=lambda x: x.start)
        for idx in range(len(self.sentences)):
            if self.sentences[idx] == s:
                if idx < len(self.sentences) - 1:
                    return self.sentences[idx + 1]
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
        super().__init__()
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
        super().__init__()
        self._text = text

    def read_full_text(self, text_key):
        return self._text


class DBTextReader(TextReader):
    def __init__(self, sql_temp, dbcnn_file):
        super().__init__()
        self._qt = sql_temp
        self._cnn_file = dbcnn_file
        raise Exception('database reader is not supported')

    def read_full_text(self, text_key):
        pass


class ESTextReader(TextReader):
    def __init__(self, es, full_text_field, patient_id_field=None):
        super().__init__()
        self._es = es
        self._text_field = full_text_field
        self._pid_field = patient_id_field

    def read_full_text(self, text_key):
        doc = self._es.get_doc_detail(text_key)
        if doc is not None:
            return {'text': doc[self._text_field], 'pid': doc[self._pid_field]}
        else:
            return None


class DocCohort(object):
    def __init__(self, d2p, processed_anns_folder, doc_id_pattern=r'(.*).json'):
        self._d2p = d2p
        self._doc_pth = processed_anns_folder
        self._did_pattern = doc_id_pattern
        self._stys = None

    @property
    def collect_semantic_types(self):
        return self._stys

    @collect_semantic_types.setter
    def collect_semantic_types(self, value):
        self._stys = value

    def collect_result(self, output_file, graph_file_path):
        files = [f for f in listdir(self._doc_pth) if isfile(join(self._doc_pth, f))]
        f_did = []
        for f in files:
            sr = re.search(self._did_pattern, f, re.IGNORECASE)
            if sr:
                f_did.append((f, sr.group(1)))
        results = []
        logging.info('collecting results ...')
        utils.multi_thread_tasking(lst=f_did, num_threads=10, process_func=DocCohort.collect_doc_anns_by_types,
                                   args=[self._doc_pth, self.collect_semantic_types, results])
        logging.info('total anns collected %s' % len(results))
        ret = {'concepts': {}, 'p2c': {}}
        for r in results:
            if r['d'] in self._d2p:
                p = self._d2p[r['d']]
                if p not in ret['p2c']:
                    ret['p2c'][p] = {}
                pd = ret['p2c'][p]
                if r['cui'] not in ret['concepts']:
                    ret['concepts'][r['cui']] = r['pref']
                if r['cui'] not in pd:
                    pd[r['cui']] = 1
                else:
                    pd[r['cui']] += 1
            else:
                logging.error('doc %s not in cohort map' % r['d'])
        utils.save_json_array(ret, output_file)
        utils.save_json_array(DocCohort.result_to_graph(ret), graph_file_path)
        logging.info('result collected')

    @staticmethod
    def collect_doc_anns_by_types(doc_tuple, dir_path, sem_types, container):
        doc = utils.load_json_data(join(dir_path, doc_tuple[0]))
        ann_doc = SemEHRAnnDoc(file_key=doc_tuple[1])
        ann_doc.load(doc, doc_tuple[1])
        for a in ann_doc.annotations:
            if (sem_types is not None and a.sty in sem_types) \
                    and a.negation == 'Affirmed' and a.experiencer == 'Patient' \
                    and len(a.ruled_by) == 0:
                container.append({'d': doc_tuple[1], 'cui': a.cui, 'pref': a.pref})
            else:
                logging.debug('%s not in %s' % (a.sty, sem_types))

    @staticmethod
    def result_to_graph(result):
        p2c = result['p2c']
        c2lbl = result['concepts']
        g = []
        for p in p2c:
            c2f = p2c[p]
            for c in c2f:
                g.append([p, c2lbl[c], c2f[c]])
        return sorted(g, key=lambda x: x[1].lower())


def process_doc_rule(ann_doc, rule_executor, reader, text_key, study_analyzer_inst, reset_prev_concept=False):
    study_concepts = study_analyzer_inst.study_concepts if study_analyzer_inst is not None else None
    num_concepts = 0
    text = None
    rule = None
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
                text = reader.read_full_text(text_key)  # .replace('\n', ' ')
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
                    context_text = text[sent.start + offset:sent.end + offset]
                    logging.debug('context text: %s' % context_text)
                s_before = context_text[:offset_start]

                # gate has an issue with splitting sentences with a question mark in the middle
                # which is quite often in clinical notes to specify not sure for a condition
                # so, if the previous sentence ends with a question mark, then bring it in for ruling
                prev_s = ann_doc.get_prev_sent(sent)
                if prev_s is not None:
                    prev_s_text = text[prev_s.start + offset:prev_s.end + offset]
                    if prev_s_text.endswith('?') or prev_s_text.lower().endswith('e.g.'):
                        s_before = prev_s_text + s_before
                        anchor_sent = prev_s

                if context_text.startswith('s '):  # or s_before == '' :
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
                                                           ann.start + offset, ann.end + offset,
                                                           more_context_sents=more_context_sents)
                if ruled:
                    ann.add_ruled_by(rule)
                    logging.info('%s [%s, %s] ruled by %s' % (str_orig, ann.start, ann.end, rule))
            else:
                logging.error('sentence not found for ann %s,%s %s' % (ann.start, ann.end, ann.str))
            num_concepts += 1
    return num_concepts


def db_doc_process(cnn, row, sql_template, pks, update_template, dbcnn_file, text_reader, sa, ruler,
                   update_status_template):
    raise Exception('db_doc_process is not supported')


def analyse_db_doc_anns(sql, ann_sql, pks, update_template, full_text_sql, dbcnn_file, rule_config_file,
                        study_folder, thread_num=10, study_config='study.json', update_status_template=None):
    raise Exception('analyse_db_doc_anns is not supported')


def analyse_doc_anns_file(ann_doc_path, rule_executor, text_reader, output_folder,
                          fn_pattern='se_ann_%s.json', es_inst=None, es_output_index=None, es_output_doc='doc',
                          study_analyzer_inst=None):
    p, fn = split(ann_doc_path)
    file_key = splitext(fn)[0]
    json_doc = utils.load_json_data(ann_doc_path)
    return analyse_doc_anns(json_doc, file_key, rule_executor, text_reader, output_folder,
                            fn_pattern, es_inst, es_output_index, es_output_doc,
                            study_analyzer_inst)


def analyse_doc_anns_line(line, rule_executor, text_reader, output_folder,
                          fn_pattern='se_ann_%s.json', es_inst=None, es_output_index=None, es_output_doc='doc',
                          study_analyzer_inst=None):
    json_doc = json.loads(line)
    file_key = json_doc['docId']
    return analyse_doc_anns(json_doc, file_key, rule_executor, text_reader, output_folder,
                            fn_pattern, es_inst, es_output_index, es_output_doc,
                            study_analyzer_inst)


def analyse_doc_anns(json_doc, file_key, rule_executor, text_reader, output_folder,
                     fn_pattern='se_ann_%s.json', es_inst=None, es_output_index=None, es_output_doc='doc',
                     study_analyzer_inst=None, contextualised_concept_index='semehr_ctx_concepts',
                     ctx_doc_type='ctx_concept'):
    ann_doc = SemEHRAnnDoc()
    ann_doc.load(json_doc, file_key=file_key)
    read_obj = text_reader.read_full_text(ann_doc.file_key)
    patient_id = None
    if isinstance(read_obj, dict):
        text = read_obj['text']
        patient_id = read_obj['pid']
    else:
        text = read_obj
    if text is None:
        logging.error('file [%s] full text not found' % ann_doc.file_key)
        return
    reader = WrapperTextReader(text)
    process_doc_rule(ann_doc, rule_executor, reader, None, study_analyzer_inst)
    if es_inst is None:
        utils.save_json_array(ann_doc.serialise_json(), join(output_folder, fn_pattern % ann_doc.file_key))
    else:
        data = ann_doc.serialise_json()
        data['doc_id'] = file_key
        data['patient_id'] = patient_id
        es_inst.index_new_doc(index=es_output_index, doc_type=es_output_doc,
                              data=data, doc_id=file_key)
        # index conceptualised concepts
        if contextualised_concept_index is not None:
            for ann in data['annotations']:
                index_ctx_concept(ann, contextualised_concept_index, ctx_doc_type, es_inst)

    return ann_doc.serialise_json()


def index_ctx_concept(ann, concept_index, ctx_doc_type, es_inst):
    raise Exception('indexing contextualised concepts not supported in this version')


def load_study_ruler(study_folder, rule_config_file, study_config='study.json'):
    sa = None
    if study_folder is not None and study_folder != '':
        r = utils.load_json_data(join(study_folder, study_config))

        ret = study_analyzer.load_study_settings(study_folder,
                                                 umls_instance=None,
                                                 rule_setting_file=r['rule_setting_file'],
                                                 concept_filter_file=None if 'concept_filter_file' not in r else r[
                                                     'concept_filter_file'],
                                                 do_disjoint_computing=True if 'do_disjoint' not in r else r[
                                                     'do_disjoint'],
                                                 export_study_concept_only=False if 'export_study_concept' not in r else
                                                 r['export_study_concept']
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
                     thread_num=10, es_inst=None, es_text_field='', patient_id_field='', combined_anns=None,
                     es_output_index=None, es_output_doc='doc'):
    """
    multiple threading process doc anns
    :type thread_num: object
    :param patient_id_field: 
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
        text_reader = ESTextReader(es_inst, es_text_field, patient_id_field=patient_id_field)
    ret = load_study_ruler(study_folder, rule_config_file, study_config)
    sa = ret['sa']
    ruler = ret['ruler']

    # for ff in [f for f in listdir(anns_folder) if isfile(join(anns_folder, f))]:
    #     analyse_doc_anns(join(anns_folder, ff), ruler, text_reader, output_folder, fn_pattern, sa)
    if combined_anns is None:
        utils.multi_thread_process_files(dir_path=anns_folder,
                                         file_extension='json',
                                         num_threads=thread_num,
                                         process_func=analyse_doc_anns_file,
                                         args=[ruler, text_reader, output_folder, fn_pattern,
                                               es_inst, es_output_index, es_output_doc,
                                               sa])
    else:
        ann_files = [f for f in listdir(anns_folder) if isfile(join(anns_folder, f))]
        for ann in ann_files:
            utils.multi_process_large_file_tasking(
                large_file=join(anns_folder, ann),
                process_func=analyse_doc_anns_line,
                args=[ruler, text_reader, output_folder, fn_pattern,
                      es_inst, es_output_index, es_output_doc,
                      sa])

    logging.info('post processing of ann docs done')


def db_populate_patient_result(container, pid, doc_ann_sql_temp, doc_ann_pks, dbcnn_file, concept_list,
                               cui2concept,
                               ontext_filter_fun=None):
    raise Exception('db_populate_patient_result is not supported')


def positive_patient_filter(ann):
    return ann.negation == 'Affirmed' and ann.experiencer == 'Patient'


def fix_escaped_issue(s):
    p = re.compile(r'(string_orig":"|pref": "|PREF":"|str": ")(((?!","|"}|", ").)*)(","|"}|", ")')
    fiz = []
    for m in p.finditer(s):
        vg = 2
        val = s[m.span(vg)[0]:m.span(vg)[1]]
        if '"' in val:
            new_val = val.replace('\\', '').replace('"', '\\"')
            fix = {'s': m.span()[0], 'e': m.span()[1],
                   'mid': s[m.span(1)[0]:m.span(1)[1]] + new_val + s[m.span(4)[0]:m.span(4)[1]]}
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


def extract_sample(pk_vals, concept, cui2concept, sample_sql_temp, dbcnn_file, container,
                   ontext_filter_fun=positive_patient_filter):
    raise Exception('extract_sample is not supported')


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
    raise Exception('db_populate_study_results is not supported')


def gen_grouped_output(c2f, p, g, grp2output, concept_list, c2pks, head):
    if g in grp2output:
        grp_str = grp2output[g]
    else:
        grp_str = head
    grp_str += get_c2f_output(p, c2f, concept_list, c2pks)
    grp2output[g] = grp_str


def get_c2f_output(p, c2f, concept_list, c2pks):
    pr = [p]
    for c in concept_list:
        if c2f[c]['f'] > 0:
            c2pks[c].append(c2f[c]['docs'][0])
        pr.append(str(c2f[c]['f']))
    return '\t'.join(pr) + '\n'


def init_study_config(study_folder):
    load_study_ruler(study_folder, None)


if __name__ == "__main__":
    init_study_config('./studies/ktr_charlson')
