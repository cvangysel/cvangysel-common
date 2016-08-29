import sklearn
import sklearn.neighbors

nn_impls = ('ball_tree', 'kd_tree')


def neighbors_algorithm(metric):
    """
    Return the name of the most efficient nearest neighbor algorithm.

    Not all algorithms support all metrics, therefore this function
    first checks whether BallTree offers support, followed by the
    KD-Tree and lastly falls back on the brute-force implementation.

    If both BallTree and KD-Tree support the metric, this function
    lets scikit-learn choose the best implementation, based on your
    data.
    """
    supported_algorithms = []

    for impl in nn_impls:
        valid_metrics = getattr(sklearn.neighbors, impl).VALID_METRIC_IDS

        if metric in valid_metrics:
            supported_algorithms.append(impl)

    if supported_algorithms:
        if set(nn_impls) == set(supported_algorithms):
            return 'auto'
        else:
            return supported_algorithms[0]
    else:
        return 'brute'
