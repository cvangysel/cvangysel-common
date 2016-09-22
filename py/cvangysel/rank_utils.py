import numpy as np
import logging
import scipy
import scipy.optimize


def generate_ranks(scores, axis=0):
    ranks = np.argsort(np.argsort(-scores, axis=axis), axis=axis)

    return ranks


def compute_dcg(ranks_starting_from_zero, relevances, rank_cutoff=None):
    ranks = ranks_starting_from_zero + 1

    pointwise_dcg = np.where(
        ranks == 1,
        relevances,
        np.array(relevances / np.log2(ranks)))

    if rank_cutoff:
        pointwise_dcg = np.where(ranks <= rank_cutoff, pointwise_dcg, 0.0)

    return pointwise_dcg.sum(axis=0).ravel()


def compute_ndcg(ranks, relevances, **kwargs):
    ranks_according_to_relevance = generate_ranks(relevances, axis=0)

    dcg = compute_dcg(ranks, relevances, **kwargs)
    ideal_dcg = compute_dcg(ranks_according_to_relevance, relevances, **kwargs)

    if np.all(ideal_dcg == 0.0):
        raise RuntimeError()

    ndcg = np.where(ideal_dcg > 0.0, dcg / ideal_dcg, 1.0)

    return ndcg


def compute_ndcg_for_document_scores(document_scores, document_relevances,
                                     **kwargs):
    assert document_scores.shape == document_relevances.shape

    document_ranks = generate_ranks(document_scores, axis=0)
    ndcg = compute_ndcg(document_ranks, document_relevances, **kwargs)

    return ndcg


def _optimal_weight_vector_optimization_objective(
        weights, features, relevances, rank_cutoff):
    if not hasattr(_optimal_weight_vector_optimization_objective, '_scores_buffer'):
        _optimal_weight_vector_optimization_objective._scores_buffer = \
            np.ndarray(shape=(features.shape[0],),
                       dtype=weights.dtype,
                       order='C')

    scores = np.dot(features, weights,
                    out=_optimal_weight_vector_optimization_objective._scores_buffer)

    ndcg = compute_ndcg_for_document_scores(
        scores, relevances, rank_cutoff=rank_cutoff)

    return -ndcg


def optimal_weight_vector(document_features, document_relevances,
                          rank_cutoff=None):
    num_documents, num_features = document_features.shape
    assert document_relevances.shape == (num_documents,)

    args = (document_features, document_relevances, rank_cutoff)

    low, high, increment = -1.0, 1.0, 0.1

    num_values_per_parameter = int((high - low) / increment) + 1

    logging.info(
        'Performing grid search for %d parameters (%d configurations).',
        num_features,
        int(np.power(num_values_per_parameter, num_features)))

    ranges = tuple(slice(low, high, increment)
                   for _ in range(num_features))

    x0 = scipy.optimize.brute(
        _optimal_weight_vector_optimization_objective,
        ranges=ranges,
        args=args,
        full_output=False)

    return x0
