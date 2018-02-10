import utils


"""
a script for supporting GeL phenome model, 
e.g., human phenotype ontology mapping generation
"""


def parse_disease_phenotypes(disease_phenotype_csv, disease_model_json):
    lines = utils.read_text_file(disease_phenotype_csv)
    dis_to_data = {}
    for l in lines[1:]:
        arr = l.split(',')
        lv4_id = arr[4]
        lv4_disease = arr[5]
        hpo_label = arr[7]
        hpo_id = arr[8]
        test = arr[9]
        test_id = arr[10]
        dis_data = []
        if lv4_disease in dis_to_data:
            dis_data = dis_to_data[lv4_disease]
        else:
            dis_to_data[lv4_disease] = dis_data
        if len(test.strip()) > 0:
            dis_data.append({'test': test,
                             'test_id': test_id})
        else:
            dis_data.append({'hpo_label': hpo_label,
                             'hpo_id': hpo_id})
    utils.save_json_array(dis_to_data, disease_model_json)


if __name__ == "__main__":
    csv = '/Users/honghan.wu/git/autoimmune-kconnect/resources/100k/Rare Disease Conditions Phenotypes and Clinical Tests  - v1.8.1.csv'
    model_file = '/Users/honghan.wu/git/autoimmune-kconnect/resources/100k/diseae_model.js'
    parse_disease_phenotypes(csv, model_file)
