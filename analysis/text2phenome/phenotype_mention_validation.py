import utils
import sqldbutils as dutils
import random

template_concept_ann_query = """
select cn_doc_id, start_offset, end_offset, string_orig from 
physical_phenotype_annotations where inst_uri='{concept}'
"""


def random_extract_mentions(cui, db_conf, query_template, sample_size=200):
    rows = []
    dutils.query_data(query_template.format(**{'concept': cui}),
                      rows,
                      dutils.get_db_connection_by_setting(db_conf))
    if len(rows) <= sample_size:
        return rows
    else:
        sampled = []
        for i in xrange(sample_size):
            index = random.randrange(len(rows))
            sampled.append(rows[index])
            rows.pop(index)
        return sampled


if __name__ == "__main__":
    print random_extract_mentions('', '', template_concept_ann_query, sample_size=10)