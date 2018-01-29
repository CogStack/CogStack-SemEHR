import joblib as jl

_STOP_POS_TAGS = ['SPACE', 'VERB', 'PUNCT', 'CCONJ', 'ADP']


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
                m_ptn = self._pattern_to_inst[ptn][0].get_minimal_component()
                arr = mini_ptns[m_ptn] if m_ptn in mini_ptns else []
                arr.append(inst)
                mini_ptns[m_ptn] = arr
        for p in mini_ptns:
            correct_ptns = []
            incorrect_ptns = []
            for inst in mini_ptns[p]:
                if inst.annotations[0]['gt_label'] != '-':
                    correct_ptns.append(inst.sentence) \
                        if inst.annotations[0]['gt_label'] == inst.annotations[0]['signed_label'] \
                        else incorrect_ptns.append(inst.sentence)
            print '%s, #insts %s, accuracy:%s' % \
                  (p, len(mini_ptns[p]),
                   1.0 * len(correct_ptns) / (len(correct_ptns) + len(incorrect_ptns))
                   if len(correct_ptns) + len(incorrect_ptns) > 0 else '-')
            if len(correct_ptns) + len(incorrect_ptns) > 20:
                print '**correct: %s' % correct_ptns
                print '**incorrect: %s' % incorrect_ptns
            print '--\n\n'

    def serialise(self, save_file_path):
        jl.dump(self, save_file_path)

    @staticmethod
    def load_seralisation(load_file_path):
        return jl.load(load_file_path)


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
            return None
        else:
            # print 'anned token: %s(%s)' % (first_ann_token, first_ann_token.i)
            # print 'children [%s]' % [t.text for t in first_ann_token.children]
            parents = [first_ann_token]
            self.iter_get_children(first_ann_token, parents)
            prev_p = first_ann_token
            p = first_ann_token.head
            while p != prev_p:
                self.iter_get_children(p, parents)
                if p.pos_ == 'VERB' or p.pos_ == 'CCONJ':
                    break
                parents.append(p)
                prev_p = p
                p = p.head
            # print 'print parents: [%s]' % ['%s(%s, %s)' % (p.text, p.pos_, p.i) for p in parents]
            arr_ptns = [(p.pos_, p.i, p.text) for p in parents if p.i >= first_ann_token.i]
            arr_ptns = sorted(arr_ptns, cmp=lambda x1, x2: x1[1] - x2[1])
            generalised_ptn = self.remove_line_separated_tokens([p for p in arr_ptns])
            # print generalised_ptn

            # simplify
            s_ptns = []
            for p in self.simplify_minimal_pattern(generalised_ptn):
                if len(s_ptns) > 0 and s_ptns[-1] == p[0]:
                    continue
                s_ptns.append(p[0])
            return '->'.join(s_ptns)

        # print '--\n\n'

    def remove_line_separated_tokens(self, ptn_list):
        for t in self._sent_model:
            if t.dep_ == u'' and ptn_list[0][1] <= t.i <= ptn_list[len(ptn_list) - 1][1]:
                return [p for p in ptn_list if p[1] <= t.i]
        return ptn_list

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