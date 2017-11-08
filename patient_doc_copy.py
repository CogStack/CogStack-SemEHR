import entity_centric_es as ees
import sys
import urllib3

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    index_setting_file = ''
    src_index = ''
    src_doc_type = ''
    entity_id_field_name = ''
    dest_index = ''
    dest_doc_type = ''
    doc_list_file = ''
    ees.copy_docs(index_setting_file, src_index, src_doc_type, entity_id_field_name,
                  dest_index, dest_doc_type, doc_list_file)
