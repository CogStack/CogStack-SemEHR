import joblib as jl
import en_core_web_sm as enmodel


_STOP_POS_TAGS = ['SPACE', 'VERB', 'PUNCT', 'CCONJ', 'ADP']
_en_model = None

class SentencePatternInst(object):
    def __init__(self):
        pass

    @property
    def sentence(self):
        return self._sentence

    @sentence.setter
    def sentence(self, value):
        self._sentence = value

    @property
    def pattern(self):
        return self._pattern


class CorpusAnalyzer(object):
    """
    A corpus analyzer - a container of annotated pattern instances that provides corpus level analytic functions
    """
    def __init__(self):
        self._pattern_to_inst = {}
        self._mini_comp_dict = None

    def add_pattern(self, pattern_inst):
        p = pattern_inst.pattern
        if p in self._pattern_to_inst:
            p_insts = self._pattern_to_inst[p]
        else:
            p_insts = []
            self._pattern_to_inst[p] = p_insts
        p_insts.append(pattern_inst)
        # p_insts.append({'sentence': pattern_inst.sentence, 'annotations': pattern_inst.annotations})

    @property
    def pattern_to_insts(self):
        return self._pattern_to_inst

    def show(self):
        arr = [(p, len(self._pattern_to_inst[p])) for p in self._pattern_to_inst]
        arr = sorted(arr, cmp=lambda x, y: y[1] - x[1])
        print 'total num patterns %s' % len(arr)
        for pt in arr[:10]:
            print '(Freq: %s) %s\n[%s]\n\n' % (pt[1], pt[0], self._pattern_to_inst[pt[0]][0].sentence)

    def get_simple_patterns(self, lenghth=5):
        return [p for p in self._pattern_to_inst if len(p.split('->')) <= lenghth]

    def test_pattern(self, pattern):
        for ptn in self.pattern_to_insts:
            if ptn.find(pattern) >= 0:
                self._pattern_to_inst[ptn][0].get_minimal_component()

    def merge_by_mini_comp(self):
        """
        generate a dictionary from minimal components to pattern instances.
        :return:
        """
        mini_ptns = {}
        for ptn in self.pattern_to_insts:
            for inst in self.pattern_to_insts[ptn]:
                m_ptn, text_comps = inst.get_minimal_component()
                if m_ptn is not None and m_ptn.find('KCL') < 0:
                    print m_ptn, inst.sentence
                arr = mini_ptns[m_ptn] if m_ptn in mini_ptns else []
                arr.append(inst)
                mini_ptns[m_ptn] = arr
        CorpusAnalyzer.print_dictionary_anns(mini_ptns)
        return mini_ptns

    def show_mini_comp_patterns(self):
        if self._mini_comp_dict is None:
            print 'minimal component dictionary is not available yet'
        CorpusAnalyzer.print_dictionary_anns(self._mini_comp_dict)

    def get_mini_comp_pattern_by_freq(self, freq=5):
        mini_comp = self.get_mini_comp_dict()
        freq_ptns = []
        for c in mini_comp:
            if len(mini_comp[c]) > freq:
                freq_ptns.append((c, len(mini_comp[c])))
        return freq_ptns

    def analyse_mini_comp_pattern(self, mini_ptn, variant_idxs):
        """
        analyse minimal component pattern instances to figure out
        which ones are easy and which are hard
        :param mini_ptn:
        :param variant_idxs:
        :return:
        """
        if self._mini_comp_dict is None:
            print 'minimal component dictionary is not available yet'
        var_dict = self.get_mini_pattern_var_dict(mini_ptn, variant_idxs)
        variant_insts = var_dict['dict']
        all_accuracy = var_dict['accuracy']
        print 'studying pattern [%s] that has %s variants covering %s instances' % (mini_ptn, len(variant_insts),
                                                                                    len(self._mini_comp_dict[mini_ptn]))
        # CorpusAnalyzer.print_dictionary_anns(variant_insts)

        # 1. clustering instances
        # - using frequent instances as basic clusters;
        freq_threshold = 5
        freq_patterns = []
        infreq_patterns = []
        for ptn in variant_insts:
            freq_patterns.append(ptn) if len(variant_insts[ptn]) >= freq_threshold else infreq_patterns.append(ptn)
        #
        # # word2vec similarities seem not very good at linking clinical related concepts
        # # UMLS semantic type might play a better role here?
        # nlp = CorpusAnalyzer.load_en_model()
        # freq_docs = []
        # for fp in freq_patterns:
        #     freq_docs.append(nlp(fp))
        # print freq_patterns
        # for ptn in infreq_patterns:
        #     d1 = nlp(ptn)
        #     sims = []
        #     i = 0
        #     for f_doc in freq_docs:
        #         sims.append((i, d1.similarity(f_doc)))
        #         i += 1
        #     sims = sorted(sims, lambda x1, x2: 1 if x2[1] - x1[1] >= 0 else -1)
        #     if sims[0][1] > .8:
        #         print '%s -> %s, %s' % (ptn, freq_patterns[sims[0][0]], sims[0][1])

        # 2. organise significant clusters by performances
        # - easy ones: high/low accuracy [0, .25] and [.75, 1]
        # - hard ones: (.25, .75)
        easy_correct = []
        easy_incorrect = []
        difficulties = []
        for fp in freq_patterns:
            num_correct = 0
            num_incorrect = 0
            for inst in variant_insts[fp]:
                if inst.annotations[0]['gt_label'] != '-':
                    if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label']:
                        num_correct += 1
                    else:
                        num_incorrect += 1
            if num_correct + num_incorrect > 0:
                accuracy = num_correct * 1.0 / (num_correct + num_incorrect)
                if accuracy >= .75:
                    easy_correct.append((fp, accuracy, len(variant_insts[fp])))
                elif accuracy <= .25:
                    easy_incorrect.append((fp, accuracy, len(variant_insts[fp]))
                                          )
                else:
                    difficulties.append((fp, accuracy, len(variant_insts[fp])))
        print 'easy correct ones \n%s ' % easy_correct
        print 'easy incorrect ones \n%s ' % easy_incorrect
        print 'hard ones ones \n%s ' % difficulties
        print 'infrequent variants [%s] - %s' % (len(infreq_patterns), infreq_patterns)
        print '---------------\n'

        # 3. hard ones need further sentence level information
        return {'mini_comp_pattern': mini_ptn,
                'all_accuracy': all_accuracy,
                'instance_number': len(self._mini_comp_dict[mini_ptn]),
                'variation_pos': variant_idxs,
                'infrequent_variants': infreq_patterns,
                'easy_accurate': easy_correct,
                'easy_inaccurate': easy_incorrect,
                'hard': difficulties}

    def get_mini_pattern_var_dict(self, mini_ptn, variant_idxs):
        if self._mini_comp_dict is None:
            print 'minimal component dictionary is not available yet'
        variant_insts = {}
        num_correct = 0
        num_incorrect = 0

        for inst in self._mini_comp_dict[mini_ptn]:
            ptn, comp_texts = inst.get_minimal_component()
            key_arr = []
            if len(variant_idxs) > 0:
                prev_idx = variant_idxs[0]
                for idx in variant_idxs:
                    if idx >= len(comp_texts):
                        break
                    if idx > prev_idx + 1:
                        key_arr += ['=#=']
                    key_arr += comp_texts[idx]
                    prev_idx = idx
            key = ' '.join(key_arr)
            arr = variant_insts[key] if key in variant_insts else []
            arr.append(inst)
            variant_insts[key] = arr
            if inst.annotations[0]['gt_label'] != '-':
                if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label']:
                    num_correct += 1
                else:
                    num_incorrect += 1
        all_accuracy = num_correct * 1.0 / (num_correct + num_incorrect) if num_correct + num_incorrect > 0 else '-'
        return {'dict': variant_insts, 'accuracy': all_accuracy}

    def analyse_sentence_scope(self, mini_ptn, variant_idxs, variant_text):
        var_result = self.get_mini_pattern_var_dict(mini_ptn, variant_idxs)
        var_dict = var_result['dict']
        for inst in var_dict[variant_text]:
            print inst.sentence

    @staticmethod
    def print_dictionary_anns(mini_ptns):
        print '#patterns %s' % len(mini_ptns)
        for p in mini_ptns:
            correct_ptns = []
            incorrect_ptns = []
            for inst in mini_ptns[p]:
                if inst.annotations[0]['gt_label'] != '-':
                    correct_ptns.append(inst.sentence + ' ' + inst.annotations[0]['signed_label']) \
                        if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label'] \
                        else incorrect_ptns.append(inst.sentence + ' ' + inst.annotations[0]['signed_label'])
            print '%s, #insts %s, #veryfied: %s, accuracy:%s' % \
                  (p, len(mini_ptns[p]),
                   len(correct_ptns) + len(incorrect_ptns),
                   1.0 * len(correct_ptns) / (len(correct_ptns) + len(incorrect_ptns))
                   if len(correct_ptns) + len(incorrect_ptns) > 0 else '-')
            if len(correct_ptns) + len(incorrect_ptns) > 20:
                print '**correct: %s' % correct_ptns
                print '**incorrect: %s' % incorrect_ptns
            print '--\n'

    def get_mini_comp_dict(self):
        if self._mini_comp_dict is None:
            print 'producing minimal component dictionary...'
            self._mini_comp_dict = self.merge_by_mini_comp()
        return self._mini_comp_dict

    def produce_save_comp_dict(self, mini_save_file):
        self._mini_comp_dict = self.merge_by_mini_comp()
        CorpusAnalyzer.save_minimal_component_dictionary(self._mini_comp_dict, mini_save_file)

    def load_mini_comp_dict(self, mini_save_file):
        self._mini_comp_dict = jl.load(mini_save_file)

    def serialise(self, save_file_path):
        jl.dump(self, save_file_path)

    @staticmethod
    def load_seralisation(load_file_path):
        return jl.load(load_file_path)

    @staticmethod
    def save_minimal_component_dictionary(mini_dic, mini_save_file):
        jl.dump(mini_dic, mini_save_file)

    @staticmethod
    def load_en_model():
        global _en_model
        if _en_model is None:
            _en_model = enmodel.load()
        return _en_model


class POSSentencePatternInst(SentencePatternInst):
    """
    A pattern instance - a sentence containing at least one annotation
    """
    def __init__(self, sent_model, anns):
        self._sent_model = sent_model.as_doc()
        self._anns = anns
        self._sentence = sent_model.text
        self._raw_pattern = None
        self._pattern = None

    def process(self):
        self.pos_tag_parsing()
        self.generalise()

    @property
    def annotations(self):
        return self._anns

    @property
    def sentence(self):
        return self._sentence

    def sent_model(self):
        return self._sent_model

    def pos_tag_parsing(self):
        self._raw_pattern = []
        print 'sent Doc model [%s]' % self._sent_model
        for sent in self._sent_model.sents:
            for token in sent:
                e = token.idx + len(token.text)
                is_matched = False
                for ann in self._anns:
                    if token.idx >= ann['s'] and e <= ann['e']:
                        self._raw_pattern.append((token.text, token.pos_, True, ann['signed_label'], ann['gt_label']))
                        is_matched = True
                        break
                # print '%s-%s, %s: [%s]' % (token.idx, e, token.idx, token.text)
                if not is_matched:
                    self._raw_pattern.append((token.text, token.pos_))

    def generalise(self):
        """
        simple POS tag based generalisation implementation
        :return:
        """
        self._pattern = []
        prev_tag = None
        for r in self._raw_pattern:
            this_tag = 'KCL' if len(r) > 2 else r[1]
            if prev_tag != this_tag:
                self._pattern.append(this_tag)
                prev_tag = this_tag
        # remove punctuation and tailing space chars
        while len(self._pattern) > 0 and self._pattern[-1] in [u'PUNCT', u'SPACE']:
            self._pattern.pop(-1)
        self._pattern = '->'.join(self._pattern)

    def get_token_by_text_pos(self, text, pos):
        """
        search token using its text and POS tag
        :param text:
        :param pos:
        :return:
        """
        for sent in self._sent_model.sents:
            for t in sent:
                if t.text == text and t.pos_ == pos:
                    return t
        return None

    def iter_get_children(self, token, container):
        """
        iteratively add a token's children into a container
        :param token:
        :param container:
        :return:
        """
        for c in token.children:
            if not POSSentencePatternInst.addable_indirect(c):
                break
            if c not in container:
                container.append(c)
                self.iter_get_children(c, container)

    @staticmethod
    def addable_indirect(token):
        return token.pos_ not in _STOP_POS_TAGS

    def get_minimal_component(self):
        """
        compute semantically minimal component around the annotated entity based on sentence parse tree
        :return:
        """
        # print '--'
        # print 'min comps [%s]' % self.sentence
        # for token in self._sent_model:
        #     print(token.text, token.dep_, token.head.text, token.head.pos_,
        #           [child for child in token.children])

        # locate first sighted ann token
        first_ann_token = None
        for p in self._raw_pattern:
            if len(p) > 2:
                first_ann_token = self.get_token_by_text_pos(p[0], p[1])
                break
        if first_ann_token is None:
            print 'annotated token not found!! %s, %s' % (p[0], p[1])
            print self._raw_pattern
            return None, None
        else:
            # print 'anned token: %s(%s)' % (first_ann_token, first_ann_token.i)
            # print 'children [%s]' % [t.text for t in first_ann_token.children]
            token_comps = [first_ann_token]
            self.iter_get_children(first_ann_token, token_comps)
            parents = []
            prev_p = first_ann_token
            p = first_ann_token.head
            while p != prev_p:
                if p.pos_ == 'VERB' or p.pos_ == 'CCONJ':
                    break
                if p.i >= first_ann_token.i:
                    self.iter_get_children(p, token_comps)
                parents.append(p)
                prev_p = p
                p = p.head
            # print 'print parents: [%s]' % ['%s(%s, %s)' % (p.text, p.pos_, p.i) for p in parents]
            arr_ptns = [(p.pos_, p.i, p.text) for p in token_comps] + [(p.pos_, p.i, p.text) for p in parents if p.i >= first_ann_token.i]
            arr_ptns = sorted(arr_ptns, cmp=lambda x1, x2: x1[1] - x2[1])
            generalised_ptn = self.remove_line_separated_tokens([p for p in arr_ptns], first_ann_token)
            # print generalised_ptn

            # simplify
            s_ptns = []
            tokens_list = []
            for p in self.simplify_minimal_pattern(generalised_ptn):
                if len(s_ptns) > 0 and s_ptns[-1] == p[0]: # p[0] - pos tag, p[1] - index, p[2] - text
                    tokens_list[-1].append(p[2])
                    continue
                s_ptns.append(p[0])
                tokens_list.append([p[2]])
            return '->'.join(s_ptns), tokens_list

    def remove_line_separated_tokens(self, ptn_list, ann_token):
        last_head_space = -1
        first_tail_space = 10000
        for t in self._sent_model:
            if t.i > ann_token.i:
                if t.dep_ == u'':
                    first_tail_space = t.i
                    break
            elif t.i > ptn_list[0][1]:
                if t.dep_ == u'':
                    last_head_space = t.i
            if t.i > ptn_list[len(ptn_list) - 1][1]:
                break
            # if t.dep_ == u'' and ptn_list[0][1] <= t.i <= ptn_list[len(ptn_list) - 1][1]:
            #     return [p for p in ptn_list if p[1] <= t.i]
        ret = []
        for p in ptn_list:
            if p[1] > last_head_space:
                ret.append(p)
        new_ret = []
        for p in ret:
            if p[1] < first_tail_space:
                new_ret.append(p)
        return new_ret

    def simplify_minimal_pattern(self, ptns):
        """
        replace annotated token(s) with a special tag and merge consecutive duplicate tags
        :param ptns:
        :return:
        """
        ann_tokens = []
        for r in self._raw_pattern:
            if len(r) > 2:
                ann_tokens.append((r[0], r[1]))
        s_ptn = []
        for ptn in ptns:
            to_add = ptn
            for at in ann_tokens:
                if ptn[0] == at[1] and ptn[2] == at[0]:
                    to_add = ('KCL', ptn[1], ptn[2])
                    break
            s_ptn.append(to_add)
        return s_ptn