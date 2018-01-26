import joblib as jl

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

    def show(self):
        arr = [(p, len(self._pattern_to_inst[p])) for p in self._pattern_to_inst]
        arr = sorted(arr, cmp=lambda x, y: y[1] - x[1])
        print 'total num patterns %s' % len(arr)
        for pt in arr[:10]:
            print '(Freq: %s) %s\n[%s]\n\n' % (pt[1], pt[0], self._pattern_to_inst[pt[0]][0].sentence)

    def serialise(self, save_file_path):
        jl.dump(self, save_file_path)

    @staticmethod
    def load_seralisation(self, load_file_path):
        return jl.load(load_file_path)


class POSSentencePatternInst(SentencePatternInst):
    def __init__(self, sent_model, anns):
        self._sent_model = sent_model
        self._anns = anns
        self._sentence = sent_model.text
        self._raw_pattern = None

    def process(self):
        self.pos_tag_parsing()
        self.generalise()

    def pos_tag_parsing(self):
        self._raw_pattern = []
        for token in self._sent_model:
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
