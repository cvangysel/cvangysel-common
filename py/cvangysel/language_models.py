from cvangysel import nltk_utils

import collections
import logging
import nltk
import numpy as np


class WordFrequencyIndex(object):

    """
        Usage example:
            frequency_index = language_models.WordFrequencyIndex(
                background_prob_dist)

            frequency_index.initialize(
                profile_corpus.__iter__(yield_entity_id=True))
    """

    def __init__(self, background_prob_dist):
        assert background_prob_dist is not None

        self.background_prob_dist = background_prob_dist

    def __repr__(self):
        return '<WordFrequencyIndex with background probability ' \
               'distribution {background_prob_dist}>'.format(
                   background_prob_dist=self.background_prob_dist)

    def __len__(self):
        return len(self.frequency_distribution_per_entity)

    def __iter__(self):
        return iter(self.frequency_distribution_per_entity.keys())

    def __contains__(self, entity_id):
        return entity_id in self.frequency_distribution_per_entity

    def initialize(self, doc_bow_and_ids_iterable):
        self.frequency_distribution_per_entity = {}
        self.terms = collections.OrderedDict()

        self.max_term_id = -1

        for entity_bow, entity_id in doc_bow_and_ids_iterable:
            if entity_id in self.frequency_distribution_per_entity:
                logging.warning('Duplicate entity %s; skipping.', entity_id)

                continue

            cfd = nltk_utils.EfficientFreqDist()

            for term_id, count in entity_bow:
                cfd[term_id] += count

                if term_id not in self.terms:
                    self.terms[term_id] = 0

                self.terms[term_id] += count
                self.max_term_id = max(self.max_term_id, term_id)

            self.frequency_distribution_per_entity[entity_id] = cfd

        if self.max_term_id < 0:
            self.max_term_id = None

    def num_unique_terms(self, entity_id):
        return len(self.terms)

    def num_samples_for_entity(self, entity_id):
        assert entity_id in self

        return self.frequency_distribution_per_entity[entity_id].N()

    def num_term_occurence_given_entity(self, entity_id, term_id):
        assert entity_id in self

        return self.frequency_distribution_per_entity[entity_id][term_id]

    def get_prob_dist_for_entity(self, entity_id,
                                 smoothing_method, smoothing_parameter):
        assert entity_id in self

        cfd = self.frequency_distribution_per_entity[entity_id]

        if smoothing_method == LanguageModel.ABSOLUTE_DIRICHLET:
            return nltk.probability.LidstoneProbDist(
                cfd, gamma=smoothing_parameter)
        elif smoothing_method == LanguageModel.JELINEK_MERCER:
            return nltk_utils.JelinekMercerProbDist(
                cfd,
                background_prob_dist=self.background_prob_dist,
                alpha=smoothing_parameter)
        elif smoothing_method == LanguageModel.RELATIVE_DIRICHLET:
            return nltk_utils.DirichletProbDist(
                cfd,
                background_prob_dist=self.background_prob_dist,
                mu=smoothing_parameter)
        elif smoothing_method is None:
            return nltk.probability.MLEProbDist(cfd)


class LanguageModel(object):

    """
        Smoothing methods for retrieval.

        A Study of Smoothing Methods for Language Models Applied to
        Ad Hoc Information Retrieval. Zhai et al. 2004.

        - JELINEK_MERCER creates a linear interpolation between the
            entity's language model and the marginal corpus distribution
            (i.e. by marginalizing out the random variable corresponding to
             the entity).

        - ABSOLUTE_DIRICHLET assumes that every word occurs at least alpha (not
            necessarily a natural number) times in every language model.

            The pseudo-counts are then normalized by dividing by the sum
            of all pseudo-counts in a single model.

        - RELATIVE_DIRICHLET is similar to ABSOLUTE_DIRICHLET above, but
            instead expresses the unseen counts proportional to the corpus
            language model probabilities.
    """
    (JELINEK_MERCER, ABSOLUTE_DIRICHLET, RELATIVE_DIRICHLET) = range(10, 13)
    SMOOTHING_METHODS = (JELINEK_MERCER,
                         ABSOLUTE_DIRICHLET, RELATIVE_DIRICHLET,
                         None)

    SMOOTHING_METHODS_DESCS = {
        None: 'mle',
        JELINEK_MERCER: 'jm',
        ABSOLUTE_DIRICHLET: 'absolute_dirichlet',
        RELATIVE_DIRICHLET: 'relative_dirichlet',
    }

    """
        Belief operators as described in

            Combining the Language Model and Inference Network Approaches
            to Retrieval. Metzler et al. 2004.

        AND_BELIEF corresponds to the Query Likelihood Model.
    """
    (AND_BELIEF, OR_BELIEF) = range(20, 22)
    SCORING_METHODS = (AND_BELIEF, OR_BELIEF)

    def __init__(self,
                 frequency_index,
                 scoring_method,
                 smoothing_method, smoothing_parameter=0.0):
        assert isinstance(frequency_index, WordFrequencyIndex)

        assert scoring_method in LanguageModel.SCORING_METHODS

        assert smoothing_method in LanguageModel.SMOOTHING_METHODS
        assert smoothing_parameter >= 0.0

        self.frequency_index = frequency_index

        self.scoring_method = scoring_method

        self.smoothing_method = smoothing_method
        self.smoothing_parameter = smoothing_parameter

    def __iter__(self):
        return iter(self.frequency_index)

    def __len__(self):
        return len(self.frequency_index)

    def __contains__(self, entity_id):
        return entity_id in self.frequency_index

    def __repr__(self):
        return '<LanguageModel ({frequency_index}) using ' \
               'scoring method {scoring_method}>'.format(
                   frequency_index=self.frequency_index,
                   scoring_method=self.scoring_method)

    def num_entities(self):
        return len(self)

    def num_samples_for_entity(self, entity_id):
        return self.frequency_index.num_samples_for_entity(entity_id)

    def num_unique_terms(self):
        return len(self.frequency_index.terms)

    def create_dense_representation(self, entity_id):
        assert entity_id in self

        prob_dist = self.frequency_index.get_prob_dist_for_entity(
            entity_id, self.smoothing_method, self.smoothing_parameter)

        representations = np.zeros(self.num_unique_terms())
        for idx, term_id in enumerate(self.frequency_index.terms):
            representations[idx] = \
                self.term_frequency_for_entity(entity_id, prob_dist.prob(term_id))

        return representations

    def score_entity_for_query(self, entity_id, query_bow):
        assert entity_id in self

        prob_dist = self.frequency_index.get_prob_dist_for_entity(
            entity_id, self.smoothing_method, self.smoothing_parameter)

        return LanguageModel.score_bow_for_prob_dist(
            prob_dist, query_bow, self.scoring_method)

    def term_frequency_for_entity(self, entity_id, term_id):
        assert entity_id in self

        prob_dist = self.frequency_index.get_prob_dist_for_entity(
            entity_id, self.smoothing_method, self.smoothing_parameter)

        return prob_dist.prob(term_id)

    @staticmethod
    def score_bow_for_prob_dist(prob_dist, bow, scoring_method):
        if scoring_method == LanguageModel.AND_BELIEF:
            log_joint_prob = 0.0

            for term_id, count in bow:
                log_prob = np.log(prob_dist.prob(term_id))

                if not np.isfinite(log_prob):
                    logging.debug(
                        'Term with identifier %s has zero probability.',
                        term_id)

                    log_prob = -1e-2

                log_joint_prob += count * log_prob

            return np.exp(log_joint_prob)
        elif scoring_method == LanguageModel.OR_BELIEF:
            log_joint_prob_complement = 0.0

            for term_id, count in bow:
                log_prob = np.log(1.0 - prob_dist.prob(term_id))

                if not np.isfinite(log_prob):
                    logging.debug(
                        'Term with identifier %s has zero probability.',
                        term_id)

                    log_prob = -1e-2

                log_joint_prob_complement += count * log_prob

            return 1.0 - np.exp(log_joint_prob_complement)
        else:
            raise NotImplementedError()
