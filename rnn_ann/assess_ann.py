import tensorflow as tf
import os
import time
import utils
import numpy as np


BATCH_SIZE = 8
NUM_STEPS = 41
HIDDEN_SIZE = 200
SKIP_STEP = 8
TEMPRATURE = 0.7
LR = 0.003
LEN_GENERATED = 16
TYPED_ANNS = ['posM', 'negM', 'hisM', 'otherM']
ANN_TOKEN = 'UMLS_CONCEPT_LABEL'


class AnnAssessor(object):
    def __init__(self):
        pass

    def load_model(self):
        pass


class AssessorTrainer(object):
    def __init__(self):
        self._one_dir_size = (NUM_STEPS - 1)/2
        self._vocab = None
        self._idx2word = None
        self._vocab_file = None
        self._learning_rate = 0.003
        self._encoded_output = None
        self._typed_anns = None
        self._data = None
        self._labels = None
        self._offset = 0

    @property
    def hidden_size(self):
        return HIDDEN_SIZE

    @property
    def vocabulary(self):
        return self._vocab

    @vocabulary.setter
    def vocabulary(self, value):
        self._vocab = value
        self._idx2word = {}
        for w in value:
            self._idx2word[value[w]] = w
        self._vocab[ANN_TOKEN] = len(self._vocab)
        self._idx2word[len(self._idx2word)] = ANN_TOKEN
        print(len(self._idx2word))
        self._typed_anns = []
        for t in TYPED_ANNS:
            self._typed_anns.append(self._vocab[t])

    def vocab_encode(self, tokens):
        return [self._vocab[t] + 1 for t in tokens]

    def vocab_decode(self, encoding):
        return ' '.join([self._idx2word[idx] for idx in encoding])

    @property
    def encoded_output(self):
        return self._encoded_output

    @encoded_output.setter
    def encoded_output(self, value):
        self._encoded_output = value

    @property
    def vocab_file(self):
        return self._vocab_file

    @vocab_file.setter
    def vocab_file(self, value):
        self._vocab_file = value
        self.vocabulary = utils.load_json_data(value)

    def read_data(self):
        return self.read_encoded_data(self.encoded_output, self._one_dir_size)

    @staticmethod
    def read_encoded_data(encoded_ann_ctx_file, one_dir_size):
        ann_ctxs = utils.load_json_data(encoded_ann_ctx_file)
        # dot_idx = self.vocab_encode(['.'])[0]
        for ctx in ann_ctxs:
            # prev = ctx['prev'][len(ctx['prev']) - ctx['prev'][::-1].index(dot_idx) - 1:] \
            #     if dot_idx in ctx['prev'] else ctx['prev']
            # chunk = [c + 1 for c in prev][-one_dir_size:]
            chunk = [c + 1 for c in ctx['prev']][-one_dir_size:] \
                    + [ctx['label_encoded'] + 1] # \
                    # + [c + 1 for c in ctx['next']][:one_dir_size]
            chunk += [0] * (one_dir_size + 1 - len(chunk)) # (2 * one_dir_size + 1 - len(chunk))
            yield chunk

    def process_data(self):
        encoded_ann_ctx_file = self.encoded_output
        one_dir_size = self._one_dir_size
        ann_ctxs = utils.load_json_data(encoded_ann_ctx_file)
        self._data = []
        self._labels = []
        for ctx in ann_ctxs:
            chunk = [c + 1 for c in ctx['prev']][-one_dir_size:] \
                    + self.vocab_encode([ANN_TOKEN]) \
                    + [c + 1 for c in ctx['next']][:one_dir_size]
            chunk += [0] * (NUM_STEPS - len(chunk))
            self._data.append(chunk)
            label = np.zeros(len(self._typed_anns))
            label[self._typed_anns.index(ctx['label_encoded'])] = 1
            self._labels.append(label)

    def read_bidirectional_data(self, batch_size=BATCH_SIZE):
        if self._offset >= len(self._data):
            return None, None
        batch_data = self._data[self._offset: min(self._offset + batch_size, len(self._data))]
        batch_labels = self._labels[self._offset: min(self._offset + batch_size, len(self._labels))]
        self._offset = min(self._offset + batch_size, len(self._labels))
        return batch_data, batch_labels

    def read_prev(self):
        return self.read_encoded_prev(self.encoded_output, self._one_dir_size)

    @staticmethod
    def read_encoded_prev(encoded_ann_ctx_file, one_dir_size):
        ann_ctxs = utils.load_json_data(encoded_ann_ctx_file)
        for ctx in ann_ctxs:
            chunk = [c + 1 for c in ctx['prev']][-one_dir_size:]
            yield chunk, ctx['label']

    @staticmethod
    def read_batch(stream, batch_size=BATCH_SIZE):
        batch = []
        for element in stream:
            batch.append(element)
            if len(batch) == batch_size:
                yield batch
                batch = []
        yield batch

    def init_graph(self):
        # initialise placeholders
        seq = tf.placeholder(tf.int32, [None, None])
        temp = tf.placeholder(tf.float32)

        oh_seq = tf.one_hot(seq - 1, len(self.vocabulary))
        print('vocab size: %s' % len(self.vocabulary))

        cell = tf.contrib.rnn.GRUCell(self.hidden_size)  # tf.nn.rnn_cell.GRUCell(hidden_size)
        in_state = tf.placeholder_with_default(
            cell.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size])
        # this line to calculate the real length of seq
        # all seq are padded to be of the same length which is NUM_STEPS
        length = tf.reduce_sum(tf.reduce_max(tf.sign(oh_seq), 2), 1)
        output, out_state = tf.nn.dynamic_rnn(cell, oh_seq, length, in_state)

        # fully_connected is syntactic sugar for tf.matmul(w, output) + b
        # it will create w and b for us
        logits = tf.contrib.layers.fully_connected(output, len(self.vocabulary), None)
        loss = tf.reduce_sum(tf.nn.softmax_cross_entropy_with_logits(logits=logits[:, :-1], labels=oh_seq[:, 1:]))
        # sample the next character from Maxwell-Boltzmann Distribution with temperature temp
        # it works equally well without tf.exp
        sample = tf.multinomial(tf.exp(logits[:, -1] / temp), 1)[:, 0]
        return seq, temp, oh_seq, logits, sample, loss, in_state, out_state

    def init_bidir_graph(self):
        # initialise placeholders
        x = tf.placeholder(tf.int32, [None, None])
        y = tf.placeholder(tf.float32, [None, len(self._typed_anns)])

        oh_seq = tf.one_hot(x - 1, len(self.vocabulary))
        print('vocab size: %s' % len(self.vocabulary))

        cell_fw = tf.contrib.rnn.GRUCell(self.hidden_size)
        cell_bw = tf.contrib.rnn.GRUCell(self.hidden_size)
        in_state_fw = tf.placeholder_with_default(
            cell_fw.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size])
        in_state_bw = tf.placeholder_with_default(
            cell_bw.zero_state(tf.shape(oh_seq)[0], tf.float32), [None, self.hidden_size])
        # this line to calculate the real length of seq
        # all seq are padded to be of the same length which is NUM_STEPS
        length = tf.reduce_sum(tf.reduce_max(tf.sign(oh_seq), 2), 1)
        length = tf.cast(length, tf.int32)
        outputs, out_states = tf.nn.bidirectional_dynamic_rnn(cell_fw, cell_bw, oh_seq,
                                                              sequence_length=length,
                                                              initial_state_fw=in_state_fw,
                                                              initial_state_bw=in_state_bw)
        output_fw, output_bw = outputs
        outputs = tf.concat([output_fw, output_bw], -1)

        # Hack to build the indexing and retrieve the right output.
        batch_size = tf.shape(outputs)[0]
        # Start indices for each sample
        index = tf.range(0, batch_size) * NUM_STEPS + (length - 1)
        # Indexing
        outputs = tf.gather(tf.reshape(outputs, [-1, HIDDEN_SIZE]), index)

        # fully_connected is syntactic sugar for tf.matmul(w, output) + b
        # it will create w and b for us
        # logits = tf.contrib.layers.fully_connected(outputs, len(self.vocabulary), None)

        # Define weights
        weights = {
            'out': tf.Variable(tf.random_normal([HIDDEN_SIZE, len(self._typed_anns)]))
        }
        biases = {
            'out': tf.Variable(tf.random_normal([len(self._typed_anns)]))
        }
        logits = tf.matmul(outputs, weights['out']) + biases['out']
        loss = tf.reduce_sum(tf.nn.softmax_cross_entropy_with_logits(logits=logits, labels=y))
        return x, y, oh_seq, logits, loss, (in_state_fw, in_state_bw), out_states

    def train_bidir(self):
        x, y, oh_seq, logits, loss, _, out_state = self.init_bidir_graph()

        global_step = tf.Variable(0, dtype=tf.int32, trainable=False, name='global_step')
        optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(loss, global_step=global_step)
        saver = tf.train.Saver()
        start = time.time()
        with tf.Session() as sess:
            writer = tf.summary.FileWriter('graphs/gist', sess.graph)
            sess.run(tf.global_variables_initializer())

            ckpt = tf.train.get_checkpoint_state(os.path.dirname('checkpoints/cris/checkpoint'))
            if ckpt and ckpt.model_checkpoint_path:
                saver.restore(sess, ckpt.model_checkpoint_path)

            iteration = global_step.eval()
            data_batch, label_batch = self.read_bidirectional_data()
            while data_batch is not None:
                batch_loss, _ = sess.run([loss, optimizer], {x: data_batch, y: label_batch})
                if (iteration + 1) % SKIP_STEP == 0:
                    print('Iter {}. \n    Loss {}. Time {}'.format(iteration, batch_loss, time.time() - start))
                    start = time.time()
                    saver.save(sess, 'checkpoints/cris/rnn', iteration)
                iteration += 1
                data_batch, label_batch = self.read_bidirectional_data()

    def test_bidir(self):
        x, y, oh_seq, logits, loss, _, out_state = self.init_bidir_graph()
        global_step = tf.Variable(0, dtype=tf.int32, trainable=False, name='global_step')
        optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(loss, global_step=global_step)
        saver = tf.train.Saver()
        start = time.time()
        with tf.Session() as sess:
            writer = tf.summary.FileWriter('graphs/gist', sess.graph)
            sess.run(tf.global_variables_initializer())

            ckpt = tf.train.get_checkpoint_state(os.path.dirname('checkpoints/cris/checkpoint'))
            if ckpt and ckpt.model_checkpoint_path:
                saver.restore(sess, ckpt.model_checkpoint_path)

            iteration = global_step.eval()
            data_batch, label_batch = self.read_bidirectional_data(batch_size=1)
            n_correct = 0
            n_total = 0
            n_pos_correct = 0
            n_pos_total = 0
            while data_batch is not None:
                p, c = sess.run([tf.nn.softmax(logits), y], {x: data_batch, y: label_batch})
                predicted = np.argmax(p)
                correct_label = np.argmax(c)
                if predicted == correct_label:
                    n_correct += 1
                print '%s\t%s\t%s\t%s\t%s' % (n_total, predicted, correct_label,
                                              '%.2f' % p.tolist()[0][predicted],
                                              '\t'.join(['%.2f' % p for p in p.tolist()[0]]))
                data_batch, label_batch = self.read_bidirectional_data(batch_size=1)
                n_total += 1
                if correct_label == 0:
                    n_pos_total += 1
                    if correct_label == predicted:
                        n_pos_correct += 1
            print 'accuracy: %s' % (1.0 * n_correct / n_total)
            print 'pos accuracy: %s' % (1.0 * n_pos_correct / n_pos_total)

    def train(self):
        seq, temp, oh_seq, logits, sample, loss, in_state, out_state = self.init_graph()

        global_step = tf.Variable(0, dtype=tf.int32, trainable=False, name='global_step')
        optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(loss, global_step=global_step)
        saver = tf.train.Saver()
        start = time.time()
        with tf.Session() as sess:
            writer = tf.summary.FileWriter('graphs/gist', sess.graph)
            sess.run(tf.global_variables_initializer())

            ckpt = tf.train.get_checkpoint_state(os.path.dirname('checkpoints/cris/checkpoint'))
            if ckpt and ckpt.model_checkpoint_path:
                saver.restore(sess, ckpt.model_checkpoint_path)

            iteration = global_step.eval()
            for batch in self.read_batch(self.read_data()):
                batch_loss, _, lbs = sess.run([loss, optimizer, oh_seq[:, 1:]], {seq: batch})
                if (iteration + 1) % SKIP_STEP == 0:
                    print('Iter {}. \n    Loss {}. Time {}'.format(iteration, batch_loss, time.time() - start))
                    start = time.time()
                    self.online_inference(sess, seq, sample, temp, in_state, out_state,
                                          seed=['zzzzz', "has", "both", "hiv", "and"])
                    saver.save(sess, 'checkpoints/cris/rnn', iteration)
                iteration += 1

    def online_inference(self, sess, seq, sample, temp, in_state, out_state, seed=['patient'],
                         sent_len=LEN_GENERATED):
        """ Generate sequence one character at a time, based on the previous character
        """
        sentence = seed
        print sentence
        state = None
        for _ in range(sent_len):
            batch = [self.vocab_encode(sentence)]
            feed = {seq: batch, temp: TEMPRATURE}
            # for the first decoder step, the state is None
            if state is not None:
                feed.update({in_state: state})
            index, state = sess.run([sample, out_state], feed)
            # max_prob_idxs = prob[0][0].argsort()[-3:][::-1] # np.argmax(prob[0][0])
            # print(max_prob_idxs, vocab_decode(max_prob_idxs, vocab))
            sentence.append(self.vocab_decode(index.tolist()))
        print(' '.join(sentence))

    def load_generate(self, prev):
        seq, temp, oh_seq, logits, sample, loss, in_state, out_state = self.init_graph()

        saver = tf.train.Saver()
        start = time.time()
        with tf.Session() as sess:
            writer = tf.summary.FileWriter('graphs/gist', sess.graph)
            sess.run(tf.global_variables_initializer())

            ckpt = tf.train.get_checkpoint_state(os.path.dirname('checkpoints/cris/checkpoint'))
            if ckpt and ckpt.model_checkpoint_path:
                saver.restore(sess, ckpt.model_checkpoint_path)
                # batch = [self.vocab_encode(prev)]

                ann_type_tokens = [4891, 4892, 4893, 4894]
                total = 0
                correct = 0
                for batch, label in self.read_prev():
                    feed = {seq: [batch], temp: TEMPRATURE}
                    index, state, prob = sess.run([sample, out_state,
                                                   tf.nn.softmax(logits[:, -1])],
                                                  feed)
                    probs = [prob[0][idx] for idx in ann_type_tokens]
                    pred = self.vocab_decode([ann_type_tokens[np.argmax(probs)]])
                    print(self.vocab_decode(np.asarray(batch) - 1))
                    print(label, pred, probs)
                    print('\n')

                    total += 1
                    if label == pred:
                        correct += 1
                print 'total: %s, accuracy:%s' % (total, correct * 1.0 / total)

if __name__ == "__main__":
    at = AssessorTrainer()
    at.vocab_file = './data/word_to_index_full.json'
    at.encoded_output = './data/encoded_ann_ctx_full.json'
    at.process_data()
    # at.train_bidir()
    # at.load_generate(["has", "both", "HIV", "and"])
    at.test_bidir()
