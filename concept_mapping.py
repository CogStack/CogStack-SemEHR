import utils
import json


def align_mapped_concepts(map_file, disorder_file):
    concept_map = utils.load_json_data(map_file)
    disorders = [d.strip() for d in utils.read_text_file(disorder_file)]
    exact_mapped = {}
    for d in disorders:
        if d in concept_map:
            exact_mapped[d] = concept_map[d]
        else:
            exact_mapped[d] = ""
    print json.dumps(exact_mapped)

align_mapped_concepts('./resources/autoimmune-concepts.json', './resources/auto_immune_gazetteer.txt')
