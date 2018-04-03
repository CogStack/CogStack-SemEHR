import json
import utils
from os.path import split

"""
this is a script for supporting glowing bear UI
"""


def load_tree_node_file(file_path, node_name=None):
    if node_name is None:
        p, fn = split(file_path)
        node_name = fn
    folder_node = create_folder_node('\\', node_name)
    for l in utils.read_text_file(file_path):
        leaf = create_leaf_node(folder_node['path'], l.split('\t')[0])
        folder_node['children'].append(leaf)
    return folder_node


def create_leaf_node(p_path, name):
    n = {
        "name": name,
        "path": p_path + name + '\\',
        "type": "UNKNOWN",
        "visualAttributes": [
            "LEAF",
            "ACTIVE"
        ],
        "conceptPath": p_path + name + '\\'
    }
    return n


def create_folder_node(p_path, name):
    n = {
        "name": name,
        "path": p_path + name + '\\',
        "type": "UNKNOWN",
        "visualAttributes": [
            "FOLDER",
            "ACTIVE"
        ],
        "conceptPath": p_path + name + '\\',
        "dimension": "concept",
        "children": []
    }
    return n


def create_tree_nodes():
    tree = {"tree_nodes": []}
    tree['tree_nodes'].append(load_tree_node_file('./resources/transmart/vital_signs.tsv', 'vital signs'))
    tree['tree_nodes'].append(load_tree_node_file('./resources/transmart/typed_documents.tsv', 'typed documents'))
    tree['tree_nodes'].append(load_tree_node_file('./resources/transmart/medical_profile.tsv', 'medical profiles'))
    tree['tree_nodes'].append(create_leaf_node('\\', 'Anywhere'))
    return tree


def main():
    print json.dumps(create_tree_nodes())


if __name__ == "__main__":
    main()
