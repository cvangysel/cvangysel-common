from cvangysel import language_models, nltk_utils

import collections
import gensim
import logging
import nltk

try:
    import pyndri
except ImportError:
    import warnings
    warnings.warn('pyndri not available', ImportWarning)

    pyndri = None

if pyndri is not None:
    from pyndri import extract_dictionary
    from pyndri import Dictionary as IndriDictionary

    class IndriCorpus(gensim.interfaces.CorpusABC):

        def __init__(self, index, dictionary):
            self.index = index
            self.dictionary = dictionary

        def __iter__(self, yield_id=False):
            for doc_id, text in self.get_id_and_texts():
                if yield_id:
                    yield self.dictionary.doc2bow(text), doc_id
                else:
                    yield self.dictionary.doc2bow(text)

        def get_id_and_texts(self):
            for document_id in range(self.index.document_base(),
                                     self.index.maximum_document()):
                ext_document_id, token_ids = self.index.document(document_id)

                yield ext_document_id, [token_id
                                        for token_id in token_ids
                                        if token_id in self.dictionary]

        def get_texts(self):
            for _, doc_tokens in self.get_id_and_texts():
                yield doc_tokens

        def __len__(self):
            return self.index.document_count()

    def extract_background_prob_dist(index):
        assert isinstance(index, pyndri.Index)

        logging.debug('Extracting term frequencies from index %s.', index)

        cfd = nltk_utils.EfficientFreqDist()

        for termid, count in index.get_term_frequencies().items():
            cfd[termid] += count

        prob_dist = nltk.probability.MLEProbDist(cfd)

        return prob_dist

    def create_word_frequency_index(index, internal_document_ids,
                                    background_prob_dist):
        word_frequency_index = language_models.WordFrequencyIndex(
            background_prob_dist=background_prob_dist)

        documents = [
            (int_document_id, index.document(int_document_id))
            for int_document_id in internal_document_ids]

        def _generate_candidate_documents():
            for int_document_id, (ext_document_id, tokens) in documents:
                document_bow = collections.defaultdict(int)

                for token in tokens:
                    document_bow[token] += 1

                yield sorted(document_bow.items()), ext_document_id

        word_frequency_index.initialize(_generate_candidate_documents())
        logging.debug('Initialized %s for retrieval.',
                      repr(word_frequency_index))

        return word_frequency_index
