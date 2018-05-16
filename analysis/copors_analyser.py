import discharge_summary_parser as dsp
from semquery import SemEHRES
import utils


def analyse_corpus(q, doc_type, full_text_field, reg_exp, output_file):
    es = init_es_inst()
    dsp.analyse_discharge_summaries(es, q,
                                    doc_type=doc_type,
                                    full_text_field=full_text_field,
                                    reg_exp=reg_exp,
                                    output_file=output_file)


def mapping_headings(heading_stats_file, output_file, freq_threshold=1000):
    heading_freq = utils.load_json_data(heading_stats_file)
    sorted_top_k_headings = sorted([(h, heading_freq[h]) for h in heading_freq], key= lambda x: -x[1])[:freq_threshold]
    s = ''
    for r in sorted_top_k_headings[:500:]:
        s += '%s\t%s\n' % (r[0], r[1])
    utils.save_string(s, './top500heading_discharge_summary.txt')
    utils.save_json_array(sorted_top_k_headings, output_file)


def init_es_inst():
    es_setting = {
        'es_host': '',
        'es_index': '',
        'es_doc_type': '',
        'es_concept_type': '',
        'es_patient_type': ''
    }
    es = SemEHRES.get_instance_by_setting(es_setting['es_host'],
                                          es_setting['es_index'],
                                          es_setting['es_doc_type'],
                                          es_setting['es_concept_type'],
                                          es_setting['es_patient_type'])
    # print es.get_doc_detail('1044334459', 'docs')
    # print es.search('docs', 'hepatitis')
    return es


if __name__ == "__main__":
    # discharge_query = 'document_description:"discharge notification"'
    # doc_type = 'docs'
    # full_text_field = 'body_analysed'
    # reg_exp = r'^([^\n\:]+)\:$'
    # output_file = '../ehr_stats/kch_dicharge_summ_headings.json'
    # analyse_corpus(discharge_query, doc_type, full_text_field, reg_exp, output_file)
    mapping_headings(
        '../ehr_stats/kch_dicharge_summ_headings.json',
        '../ehr_stats/kch_dicharge_summ_headings_top1k.json')