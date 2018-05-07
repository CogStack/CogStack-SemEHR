import utils
import sqldbutils as dutil
import re
import sys


class TPDBConn(object):
    def __init__(self):
        self._cnn_file = None
        self._ant_table_map = []
        self._ann_sql_template = None
        self._pt_sql_template = None

    @property
    def db_conn_file(self):
        return self._cnn_file

    @db_conn_file.setter
    def db_conn_file(self, value):
        self._cnn_file = value

    @property
    def ant_to_tables(self):
        """
        a map from annotate iteration name pattern
        to annotation table names
        :return:
        """
        return self._ant_table_map

    @ant_to_tables.setter
    def ant_to_tables(self, value):
        self._ant_table_map = value

    @property
    def ann_query_template(self):
        """
        sql query template to get NLP annotation details
        :return:
        """
        return self._ann_sql_template

    @ann_query_template.setter
    def ann_query_template(self, value):
        self._ann_sql_template = value

    @property
    def doc_patient_id_query_template(self):
        return self._pt_sql_template

    @doc_patient_id_query_template.setter
    def doc_patient_id_query_template(self, value):
        self._pt_sql_template = value

    def query_data(self, query_template, q_obj):
        rows_container = []
        dutil.query_data(query_template.format(**q_obj), rows_container,
                         dbconn=dutil.get_db_connection_by_setting(self.db_conn_file))
        return rows_container

    def query_details_by_doc_ids(self, annotator_id, doc_ids):
        """
        given a list of document IDs, the function queries to
        get two result sets:
        doc_anns - doc_id to annotation detail table;
        doc_pts - doc_id to patient id table;
        the data structure depends on the query templates
        :param annotator_id:
        :param doc_ids:
        :return:
        """
        table_settings = self.get_matched_tables(annotator_id)
        if table_settings is None:
            # raise Exception('annotator [%s] is not recognised' % annotator_id)
            print 'annotator [%s] is not recognised' % annotator_id
            return None
        # query anns for doc ids
        doc_ids_str = ','.join(["'" + d + "'" for d in doc_ids])
        doc_anns = self.query_data(self.ann_query_template, {"doc_ids": doc_ids_str,
                                                             "ann_table": table_settings['ann']})

        # query patient ids for doc ids
        doc_pts = self.query_data(self.doc_patient_id_query_template, {"doc_ids": doc_ids_str,
                                                                       "doc_table": table_settings['doc_pt']})
        return {"doc_anns": doc_anns, "doc_pts": doc_pts}

    def get_matched_tables(self, annotator_id):
        s_compare = annotator_id
        for ant_ptn in self.ant_to_tables:
            reg_p = re.compile(ant_ptn)
            m = reg_p.match(s_compare)
            if m is not None:
                # print m.group(0)
                return self.ant_to_tables[ant_ptn]
        return None

    @staticmethod
    def get_instance(conf_file):
        map = utils.load_json_data(conf_file)
        t = TPDBConn()
        t.db_conn_file = map['db_conn_file']
        t.ant_to_tables = map['ant_table_map']
        t.ann_query_template = map['ann_sql_template']
        t.doc_patient_id_query_template = map['doc_patient_id_sql_template']
        return t


def complement_feedback_data(feed_back_file, tp_conf_file, completed_file_output):
    # read feed_back_file - the dump of feedback from DB
    # in the format like:
    # 6336	d123_s1581_e1589	kate_skin_201805	posM	30/04/18 20:52
    annotator_to_anns = {}
    lines = utils.read_text_file(feed_back_file)
    for l in lines:
        cols = l.split(',')
        anns_data = cols[1].split('_')
        annotator_id = cols[2] if len(cols[2]) > 0 else 'unknown'
        doc_id = anns_data[0][1:]
        a = {}
        a['doc_id'] = doc_id
        a['start_offset'] = anns_data[1][1:]
        a['end_offset'] = anns_data[2][1:]
        a['annotator_id'] = annotator_id
        a['annotator_label'] = cols[3]
        a['annotate_time'] = cols[4]
        if annotator_id not in annotator_to_anns:
            annotator_to_anns[annotator_id] = []
        annotator_to_anns[annotator_id].append(a)

    # initialise tp instance
    tp = TPDBConn.get_instance(tp_conf_file)
    for a in annotator_to_anns:
        print '%s with %s anns' % (a, len(annotator_to_anns[a]))
        detail = tp.query_details_by_doc_ids(a, list(set([da['doc_id'] for da in annotator_to_anns[a]])))
        if detail is None:
            continue
        # process anns, populate doc->[anns] start_offset->ann
        d2anns = {}
        for ar in detail['doc_anns']:
            if ar['doc_id'] not in d2anns:
                d2anns[ar['doc_id']] = {}
            offset_to_ann = d2anns[ar['doc_id']]
            offset_to_ann[str(ar['start_offset'])] = ar

        # process doc to ann dict
        d2pts = {}
        for dp in detail['doc_pts']:
            d2pts[dp['doc_id']] = dp['patient_id']

        for labeled in annotator_to_anns[a]:
            if labeled['doc_id'] in d2anns:
                if str(labeled['start_offset']) in d2anns[labeled['doc_id']]:
                    labeled.update(d2anns[labeled['doc_id']][str(labeled['start_offset'])])
                else:
                    print '!!annotations [%s] not in found doc ann list' % labeled
            else:
                print '!!%s doc annotations not found' % labeled['doc_id']
            if labeled['doc_id'] in d2pts:
                labeled['patient_id'] = d2pts[labeled['doc_id']]
            else:
                print '!!%s doc patient id not found for %s' % labeled['doc_id']

    print 'total annotation iterations is [%s]' % len(annotator_to_anns)
    s = ''
    for a in annotator_to_anns:
        row = '\t'
        s += [ann[k] for k in ann for ann in annotator_to_anns[a]]
    s = '\n'.join([[r[k] for k in r for r in labeled]])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print 'the syntax is [python tp_hlper.py ANN_DUMP_FILE TP_CONFIGURATION_FILE OUTPUT_FILE]'
    else:
        complement_feedback_data(sys.argv[1], sys.argv[2], sys.argv[3])
