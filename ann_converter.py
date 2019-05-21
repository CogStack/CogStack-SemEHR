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
        if ann.ruled_by is not None and len(ann.ruled_by) >0:
            str_context += 'ruled_'
        return '%s%s(%s)' % (str_context, ann.pref, ann.cui)

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
            idx += 1
            mention_id = id_pattern % (ann_doc.file_key, idx)
            elem_ann = ET.SubElement(elem_annotations, "annotation")
            elem_mention = ET.SubElement(elem_ann, "mention")
            elem_mention.set('id', mention_id)
            elem_annotator = ET.SubElement(elem_ann, "annotator")
            elem_annotator.set('id', 'semehr')
            elem_annotator.text = 'semehr'
            elem_span = ET.SubElement(elem_ann, "span")
            s = ann.start
            e = ann.end
            if full_text is not None:
                if full_text[s:e].lower() != ann.str.lower():
                    os = s
                    oe = e
                    [s, e] = ann_post_rules.AnnRuleExecutor.relocate_annotation_pos(full_text,
                                                                                    s, e, ann.str)
                    logging.info('%s,%s => %s,%s' % (os, oe, s, e))
                # else:
                #    logging.info('string matches, no reloaction needed [%s] [%s]' % (full_text[s:e].lower(), ann.str.lower()))
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
            elem_mention_class.set('id', AnnConverter.get_semehr_ann_label(ann))
            elem_mention_class.text = ann.str
        tree = ET.ElementTree(elem_annotations)
        return ET.tostring(elem_annotations, encoding='utf8', method='xml')

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
                                    ann_to_convert=None):
        text_files = [f for f in listdir(full_text_folder) if isfile(join(full_text_folder, f))]
        p = re.compile(full_text_file_pattern)
        for f in text_files:
            logging.info('working on [%s]' % f)
            m = p.match(f)
            if m is not None:
                fk = m.group(1)
                text = utils.read_text_file_as_string(join(full_text_folder, f))
                anns = utils.load_json_data(join(ann_folder, ann_file_pattern % fk))
                xml = AnnConverter.to_eHOST(AnnConverter.load_ann(anns, fk), full_text=text,
                                            ann_to_convert=ann_to_convert)
                utils.save_string(xml, join(output_folder, output_file_pattern % fk))
                utils.save_string(text.replace('\r', ' '), join(full_text_folder, f))
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
            settings['ann_to_convert']
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
