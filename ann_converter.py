import docanalysis as da
import xml.etree.ElementTree as ET
import datetime
import utils
import sqldbutils as dbutils
import json
from os.path import join, isfile
from os import listdir
import logging
import sys
import ann_post_rules
import re


class AnnConverter(object):

    @staticmethod
    def load_ann(ann_json, file_key):
        d = da.SemEHRAnnDoc(file_key=file_key)
        d.load(ann_json)
        return d

    @staticmethod
    def get_semehr_ann_label(ann):
        str_context = ''
        if ann.negation != 'Affirmed':
            str_context += ann.negation + '_'
        if ann.temporality != 'Recent':
            str_context += ann.temporality + '_'
        if ann.experiencer != 'Patient':
            str_context += ann.experiencer + '_'
        if hasattr(ann, 'ruled_by'):
            if ann.ruled_by is not None and len(ann.ruled_by) >0:
                str_context += 'ruled_'
        pref = ''
        if hasattr(ann, 'pref'):
            pref = ann.pref
        elif hasattr(ann, 'minor_type'):
            pref = ann.minor_type
        cui = ''
        if hasattr(ann, 'cui'):
            cui = ann.cui
        elif hasattr(ann, 'major_type'):
            cui = ann.major_type
        return '%s%s(%s)' % (str_context, pref, cui)

    @staticmethod
    def get_simplified_ann_label(ann):
        str_context = ''
        if ann.negation != 'Affirmed':
            str_context += ann.negation
        if hasattr(ann, 'ruled_by'):
            if ann.ruled_by is not None and len(ann.ruled_by) > 0:
                if 'positive_filters.json' in ann.ruled_by:
                    str_context = ''
                elif 'negation_filters.json' in ann.ruled_by:
                    str_context = 'Negated'
        cui = ''

        if hasattr(ann, 'cui'):
            if ann.sty not in ['Disease or Syndrome']:
                return None, None
            cui = ann.sty.replace(' ', '_').replace(',', '_')
        elif hasattr(ann, 'major_type'):
            cui = ann.major_type
        return str_context, cui # '%s%s' % (str_context, cui)

    @staticmethod
    def to_eHOST(ann_doc, full_text=None, file_pattern='%s.txt', id_pattern='smehr-%s-%s',
                 ann_to_convert=None):
        elem_annotations = ET.Element("annotations")
        elem_annotations.set('textSource', file_pattern % ann_doc.file_key)
        idx = 0
        anns = []
        if ann_to_convert is None:
            ann_to_convert = ['annotations', 'phenotypes']
        if 'annotations' in ann_to_convert:
            anns += ann_doc.annotations
        if 'phenotypes' in ann_to_convert:
            anns += ann_doc.phenotypes
        for ann in anns:
            ctx, label = AnnConverter.get_simplified_ann_label(ann)
            if label is None:
                continue
            idx += 1
            mention_id = id_pattern % (ann_doc.file_key, idx)
            elem_ann = ET.SubElement(elem_annotations, "annotation")
            elem_mention = ET.SubElement(elem_ann, "mention")
            elem_mention.set('id', mention_id)
            elem_annotator = ET.SubElement(elem_ann, "annotator")
            elem_annotator.set('id', 'semehr')
            elem_annotator.text = 'semehr'
            elem_span = ET.SubElement(elem_ann, "span")
            s, e, _ = AnnConverter.get_real_start_end(full_text, ann.start, ann.end, str_orig=ann.str)
            elem_span.set('start', '%s' % s)
            elem_span.set('end', '%s' % e)
            elem_spanText = ET.SubElement(elem_ann, "spannedText")
            elem_spanText.text = ann.str
            elem_date = ET.SubElement(elem_ann, "creationDate")
            elem_date.text = datetime.datetime.now().strftime("%a %B %d %X %Z %Y")
            #
            elem_class = ET.SubElement(elem_annotations, "classMention")
            elem_class.set('id', mention_id)
            elem_mention_class = ET.SubElement(elem_class, "mentionClass")
            elem_mention_class.set('id', '%s_%s' % (ctx, label))
            elem_mention_class.text = ann.str
        tree = ET.ElementTree(elem_annotations)
        return ET.tostring(elem_annotations, encoding='utf8', method='xml')

    @staticmethod
    def get_real_start_end(full_text, s, e, str_orig):
        ss = str_orig
        if full_text is not None:
            if full_text[s:e].lower() != str_orig.lower():
                os = s
                oe = e
                [s, e] = ann_post_rules.AnnRuleExecutor.relocate_annotation_pos(full_text,
                                                                                s, e, str_orig)
                logging.info('%s,%s => %s,%s' % (os, oe, s, e))
            ss = full_text[s:e]
        return s, e, ss

    @staticmethod
    def to_brat(ann_doc, full_text=None, file_pattern='%s.txt', id_pattern='T%s',
                ann_to_convert=None, entity_type_container=None):
        anns = []
        if ann_to_convert is None:
            ann_to_convert = ['annotations', 'phenotypes']
        if 'annotations' in ann_to_convert:
            anns += ann_doc.annotations
        if 'phenotypes' in ann_to_convert:
            anns += ann_doc.phenotypes
        terms = []
        contexts = []
        for idx, ann in enumerate(anns):
            mention_id = id_pattern % (idx + 1)
            s, e, ss = AnnConverter.get_real_start_end(full_text, ann.start, ann.end, str_orig=ann.str)
            ctx, label = AnnConverter.get_simplified_ann_label(ann)
            if label is None:
                continue
            term = '%s\t%s %s %s\t%s' % (mention_id,
                                         label,
                                         s, e,
                                         ss)
            if len(ctx) > 0:
                contexts.append('A%s\t%s %s' % (idx + 1, ctx, mention_id))
            if entity_type_container is not None:
                entity_type_container.add(label)
            terms.append(term)
        return '\n'.join(terms + contexts)

    @staticmethod
    def convert_text_ann_from_db(sql_temp, pks, db_conn, full_text_folder, ann_folder,
                                 full_text_file_pattern='%s.txt',
                                 ann_file_pattern='%s.txt.knowtator.xml'):
        sql = sql_temp.format(**pks)
        results = []
        logging.info('doing [%s]...' % sql)
        file_key = '_'.join([pks[k] for k in pks])
        dbutils.query_data(sql, results, dbutils.get_db_connection_by_setting(db_conn))
        if len(results) > 0:
            text = results[0]['text'].replace('\r', '\n')
            anns = json.loads(results[0]['anns'])
            xml = AnnConverter.to_eHOST(AnnConverter.load_ann(anns, file_key), full_text=text)
            utils.save_string(xml, join(ann_folder, ann_file_pattern % file_key))
            utils.save_string(text, join(full_text_folder, full_text_file_pattern % file_key))
            logging.info('doc [%s] done' % file_key)
        else:
            logging.info('doc/anns [%s] not found' % file_key)

    @staticmethod
    def get_db_docs_for_converting(settings):
        sql = settings['sql']
        db_conn = settings['db_conn']
        doc_ann_sql_temp = settings['sql_temp']
        full_text_folder = settings['full_text_folder']
        ann_folder = settings['ann_folder']
        results = []
        dbutils.query_data(sql, results, dbutils.get_db_connection_by_setting(db_conn))
        ds = []
        for r in results:
            ds.append(r)
        logging.info('total docs %s' % len(ds))
        for d in ds:
            AnnConverter.convert_text_ann_from_db(sql_temp=doc_ann_sql_temp,
                                                  pks=d,
                                                  db_conn=db_conn,
                                                  full_text_folder=full_text_folder,
                                                  ann_folder=ann_folder)

    @staticmethod
    def convert_text_ann_from_files(full_text_folder, ann_folder, output_folder,
                                    full_text_file_pattern='(%s).txt',
                                    ann_file_pattern='se_ann_%s.json',
                                    output_file_pattern='%s.txt.knowtator.xml',
                                    ann_to_convert=None,
                                    output_type='eHOST'):
        text_files = [f for f in listdir(full_text_folder) if isfile(join(full_text_folder, f))]
        p = re.compile(full_text_file_pattern)
        entity_types = set()
        for f in text_files:
            logging.info('working on [%s]' % f)
            m = p.match(f)
            if m is not None:
                fk = m.group(1)
                text = utils.read_text_file_as_string(join(full_text_folder, f))
                anns = utils.load_json_data(join(ann_folder, ann_file_pattern % fk))
                output_str = None
                if output_type == 'eHOST':
                    output_str = AnnConverter.to_eHOST(AnnConverter.load_ann(anns, fk), full_text=text,
                                                       ann_to_convert=ann_to_convert)
                else:
                    output_str = AnnConverter.to_brat(AnnConverter.load_ann(anns, fk), full_text=text,
                                                      ann_to_convert=ann_to_convert, entity_type_container=entity_types)
                utils.save_string(output_str, join(output_folder, output_file_pattern % fk))
                utils.save_string(text.replace('\r', ' '), join(full_text_folder, f))
                if len(entity_types) > 0:
                    utils.save_string('\n'.join(entity_types), join(output_folder, 'all_entity_types.txt'))
                logging.info('doc [%s] done' % fk)

    @staticmethod
    def get_files_for_converting(settings):
        AnnConverter.convert_text_ann_from_files(
            settings['full_text_folder'],
            settings['ann_folder'],
            settings['output_folder'],
            settings['full_text_file_pattern'],
            settings['ann_file_pattern'],
            settings['output_file_pattern'],
            settings['ann_to_convert'],
            output_type=settings['output_type'] if 'output_type' in settings else 'eHOST'
        )

    @staticmethod
    def convvert_anns(setting_file):
        settings = utils.load_json_data(setting_file)
        if settings['source'] == 'db':
            AnnConverter.get_db_docs_for_converting(settings)
        else:
            AnnConverter.get_files_for_converting(settings)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    logging.basicConfig(level='INFO', format='%(name)s %(asctime)s %(levelname)s %(message)s')
    if len(sys.argv) != 2:
        print 'the syntax is [python ann_converter.py SETTING_FILE_PATH]'
    else:
        AnnConverter.convvert_anns(sys.argv[1])
