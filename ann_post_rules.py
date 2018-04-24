import re
import utils
from os.path import join

_text_window = 100
_head_text_window_size = 200


class AnnRuleExecutor(object):

    def __init__(self):
        self._text_window = _text_window
        self._filter_rules = []
        self._skip_terms = []
        self._osf_rules = []

    @property
    def skip_terms(self):
        return self._skip_terms

    @skip_terms.setter
    def skip_terms(self, value):
        self._skip_terms = value

    def add_filter_rule(self, token_offset, reg_strs, case_sensitive=False):
        self._filter_rules.append({'offset': token_offset, 'regs': reg_strs, 'case_sensitive': case_sensitive})

    def execute(self, text, ann_start, ann_end):
        s_before = text[max(ann_start - self._text_window, 0):ann_start]
        s_end = text[ann_end:min(len(text), ann_end + self._text_window)]
        # tokens_before = nltk.word_tokenize(s_before)
        # tokens_end = nltk.word_tokenize(s_end)
        # print tokens_before
        # print tokens_end
        filtered = False
        matched = []
        for r in self._filter_rules:
            s_compare = s_end if r['offset'] > 0 else s_before
            if r['offset'] == 0:
                s_compare = text[:_head_text_window_size]
            s_compare = s_compare.replace('\n', ' ')
            try:
                if r['case_sensitive']:
                    reg_p = re.compile('|'.join(r['regs']))
                else:
                    reg_p = re.compile('|'.join(r['regs']), re.IGNORECASE)
            except Exception:
                print 'regs error: [%s]' % r['regs']
                exit(1)
            # print 'matching %s on %s' % (reg_p, s_compare)
            m = reg_p.match(s_compare)
            if m is not None:
                # print m.group(0)
                matched.append([m.group(0), r['regs']])
                filtered = True
                break
        return filtered, matched

    def add_original_string_filters(self, regs):
        self._osf_rules += regs

    def execute_original_string_rules(self, string_orig):
        """
        filter the matching substring using patterns
        :param string_orig:
        :return:
        """
        s_compare = string_orig
        filtered = False
        matched = []
        for r in self._osf_rules:
            try:
                reg_p = re.compile(r)
            except Exception:
                print 'regs error: [%s]' % r['regs']
                exit(1)
            # print 'matching %s on %s' % (reg_p, s_compare)
            m = reg_p.match(s_compare)
            if m is not None:
                # print m.group(0)
                matched.append([m.group(0), r])
                filtered = True
                break
        return filtered, matched

    def load_rule_config(self, config_file):
        rule_config = utils.load_json_data(config_file)
        r_path = rule_config['rules_folder']
        print 'loading rules from [%s]' % r_path
        for rf in rule_config['active_rules']:
            for r in utils.load_json_data(join(r_path, rf)):
                self.add_filter_rule(r['offset'], r['regs'])
            print '%s loaded' % rf
        if 'osf_rules' in rule_config:
            for osf in rule_config['osf_rules']:
                self.add_original_string_filters(utils.load_json_data(join(r_path, osf)))
                print 'original string filters from [%s] loaded' % osf
        if 'skip_term_setting' in rule_config:
            self.skip_terms = utils.load_json_data(rule_config['skip_term_setting'])


def test_filter_rules():
    t = "\na close\n frined of hers died of cancer"
    t = "aaa has No evidence of cancer"
    e = AnnRuleExecutor()
    rules = utils.load_json_data('./studies/rules/negation_filters.json')
    for r in rules:
        print r
        e.add_filter_rule(r['offset'], r['regs'], case_sensitive=True if 'case' in r and r['case'] is True else False)
    print e.execute(t, 24, 30)


def test_osf_rules():
    t = "ADAD-A"
    e = AnnRuleExecutor()
    e.load_rule_config('./studies/prathiv/pirathiv_rule_config.json')
    # rules = utils.load_json_data('./studies/rules/osf_acroynm_filters.json')
    # e.add_original_string_filters(rules)
    print e.execute_original_string_rules(t)


if __name__ == "__main__":
    test_osf_rules()
