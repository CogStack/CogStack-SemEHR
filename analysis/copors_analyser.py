import discharge_summary_parser as dsp
from semquery import SemEHRES


def analyse_corpus(q, doc_type, full_text_field, reg_exp, output_file):
    es = init_es_inst()
    dsp.analyse_discharge_summaries(es, q,
                                    doc_type=doc_type,
                                    full_text_field=full_text_field,
                                    reg_exp=reg_exp,
                                    output_file=output_file)


def init_es_inst():
    es_setting = {
        'es_host': '',
        'es_index': 'mimic',
        'es_doc_type': 'eprdoc',
        'es_concept_type': 'ctx_concept',
        'es_patient_type': 'patient'
    }
    es = SemEHRES.get_instance_by_setting(es_setting['es_host'],
                                          es_setting['es_index'],
                                          es_setting['es_doc_type'],
                                          es_setting['es_concept_type'],
                                          es_setting['es_patient_type'])
    return es


if __name__ == "__main__":
    discharge_query = ''
    doc_type = ''
    full_text_field = ''
    reg_exp = r'^([^\n\:]+)\:$'
    output_file = ''
    analyse_corpus(discharge_query, doc_type, full_text_field, reg_exp, output_file)