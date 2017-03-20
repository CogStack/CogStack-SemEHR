import requests
import xml.etree.ElementTree as et
import traceback
import json
import utils
from os.path import join
import os


europepmc_full_text_url = 'http://www.ebi.ac.uk/europepmc/webservices/rest/{}/fullTextXML'
europepmc_search_url = 'http://www.ebi.ac.uk/europepmc/webservices/rest/search?' \
                       'query={term}' \
                       '%20open_access:y' \
                       '%20HAS_FT:y' \
                       '&format=json' \
                       '&pageSize={pageSize}'


def get_pmc_paper_fulltext(pmcid):
    s = ''
    full_xml = requests.get(europepmc_full_text_url.format(pmcid)).content
    try:
        root = et.fromstring(full_xml)
        # print iter_element_text(root)
        text_data = []
        telem = root.find('.//article-title')
        title = ''
        if telem is not None:
            title = ''.join(telem.itertext())
            title += '.'
            text_data.append(title)

        elem = root.find('.//abstract')
        if elem is not None:
            abstract = iterate_get_text(elem)
            text_data.append('abstract\n' + abstract)

        elem = root.find('body')
        if elem is not None:
            text_data.append(iterate_get_text(elem))
        s = '\n'.join(text_data)
    except Exception, e:
        traceback.print_exc()
    return s


def iterate_get_text(elem):
    """
    get all inner text values of this element with special cares of
    1) ignoring not relevant nodes, e.g., xref, table
    2) making section/title texts identifiable by the sentence tokenizer
    Args:
        elem:

    Returns:

    """
    remove_tags = ['table']
    line_break_tags = ['sec', 'title']
    s = ''
    if elem.tag not in remove_tags:
        local_str = elem.text.strip() + ' ' if elem.text is not None else ''
        if elem.tag in line_break_tags and elem.text is not None:
            local_str = '\n' + local_str + '.\n'
        s += local_str
        for e in list(elem):
            ct = iterate_get_text(e)
            s += (' ' + ct) if len(ct) > 0 else ''
    s += elem.tail.strip() + ' ' if elem.tail is not None else ''
    return s


def search_pmc(term, page_size=50):
    result_json = requests.get(europepmc_search_url.format(**{'term': term, 'pageSize': page_size})).content
    print result_json
    return json.loads(result_json)['resultList']['result']


def do_download_pmc_full_text(pmcid, data_folder):
    text = get_pmc_paper_fulltext(pmcid)
    utils.save_string(text, join(data_folder, pmcid))


def dump_pmc_data(term, page_size, data_path):
    docs = search_pmc(term, page_size)
    utils.save_json_array(docs, join(data_path, 'pmc_docs.json'))
    utils.multi_thread_tasking([d['pmcid'] for d in docs if 'pmcid' in d], 10, do_download_pmc_full_text,
                               args=[join(data_path, 'fulltext')])
    print 'done'


def dump_pmc_doc_ids(doc_json, file_path):
    docs = utils.load_json_data(doc_json)
    utils.save_string('\n'.join([d['pmcid'] for d in docs if 'pmcid' in d]), file_path)

if __name__ == "__main__":
    # print get_pmc_paper_fulltext('PMC5309427')
    # search_pmc('parkinson''s')
    # dump_pmc_data('parkinson''s', 20, './pubmed_test')
    dump_pmc_doc_ids('./pubmed_test/pmc_docs.json', './pubmed_test/pmc_doc_ids.txt')

