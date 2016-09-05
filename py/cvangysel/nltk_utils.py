import collections
import nltk.probability
import numpy as np

try:
    from nltk.corpus import stopwords
except ImportError:
    import nltk
    nltk.download('stopwords')

    import importlib
    importlib.reload(nltk)

    from nltk.corpus import stopwords


def get_stopwords(include_trectext_syntax=True):
    ignore_words = ['<doc>', '</doc>', '<docno>', '<text>', '</text>']

    ignore_words.extend(stopwords.words('english'))
    ignore_words.extend(stopwords.words('dutch'))

    return set(ignore_words)


class EfficientFreqDist(object):

    def __init__(self, *args, **kwargs):
        super(EfficientFreqDist, self).__init__(*args, **kwargs)

        self.partition_function = 0.0
        self.samples = collections.defaultdict(float)

    def N(self):
        return self.partition_function

    def B(self):
        return len(self.samples)

    def freq(self, sample):
        if self.N() == 0:
            return 0
        return float(self.samples[sample]) / self.N()

    def get(self, key, default=None):
        if key in self.samples:
            return self.samples[key]
        else:
            return default

    def keys(self):
        return self.samples.keys()

    def __contains__(self, key):
        return key in self.samples

    def __getitem__(self, key):
        return self.samples[key]

    def __setitem__(self, key, value):
        self.partition_function += value - self.get(key, 0)
        self.samples[key] = value

    def merge(self, other):
        assert isinstance(other, EfficientFreqDist)

        for key in other.keys():
            self[key] += other.freq(key)


class MultinomialFreqDist(EfficientFreqDist):

    def __init__(self, *args, **kwargs):
        super(MultinomialFreqDist, self).__init__(*args, **kwargs)

    def freq(self, sample):
        assert hasattr(sample, '__iter__')

        log_freq = 0.0

        for unigram in sample:
            log_freq += np.log(super(MultinomialFreqDist, self).freq(unigram))

        return np.exp(log_freq)

    def __getitem__(self, sample):
        assert hasattr(sample, '__iter__')

        return np.fromiter(
            (super(MultinomialFreqDist, self).__getitem__(observation)
             for observation in sample),
            dtype=np.float64,
            count=len(sample))

    def __setitem__(self, sample, value):
        assert hasattr(sample, '__iter__')
        assert isinstance(value, np.ndarray)

        for idx, observation in enumerate(sample):
            super(MultinomialFreqDist, self).__setitem__(
                observation, value[idx])

    def merge(self, other):
        assert isinstance(other, MultinomialFreqDist)

        for key in other.keys():
            super(MultinomialFreqDist, self).__setitem__(
                key,
                super(MultinomialFreqDist, self).__getitem__(key) +
                super(MultinomialFreqDist, other).freq(key))


def create_background_corpus_prob_dist(corpus):
    cfd = EfficientFreqDist()

    for docno, bow in enumerate(corpus):
        for termid, count in bow:
            cfd[termid] += count

    prob_dist = nltk.probability.MLEProbDist(cfd)

    return prob_dist


def extract_probs(prob_dist, domain):
    assert isinstance(domain, list)

    probs = np.zeros(len(domain), dtype=np.float64)

    for idx, observation in enumerate(domain):
        probs[idx] = prob_dist.prob(observation)
        assert probs[idx] > 0.0, observation

    probs /= probs.sum()

    return probs


def generate_samples(num_samples, probs, domain):
    assert isinstance(domain, list)
    assert isinstance(probs, np.ndarray)

    assert len(domain) == probs.size

    samples = np.random.multinomial(num_samples, probs)

    for idx, observation in enumerate(domain):
        for repeats in range(samples[idx]):
            yield observation


class UniformProbDist(nltk.probability.MLEProbDist):
    """The non-informative distribution."""

    def __init__(self, num_bins):
        assert num_bins > 0

        self.num_bins = num_bins
        self.p = np.exp(-np.log(num_bins))

    def prob(self, sample):
        return self.p

    def __repr__(self):
        return '<UniformProbDist>'


class JelinekMercerProbDist(nltk.probability.MLEProbDist):
    """Implements Jelinek-Mercer smoothing."""

    def __init__(self, freqdist, background_prob_dist, alpha):
        super(JelinekMercerProbDist, self).__init__(freqdist)

        assert alpha >= 0.0 and alpha <= 1.0

        self._background_prob_dist = background_prob_dist
        self.alpha = alpha

    def prob(self, sample):
        return float(self.alpha) * self._freqdist.freq(sample) + \
            (1.0 - float(self.alpha)) * self._background_prob_dist.prob(sample)

    def __repr__(self):
        return '<JelinekMercerProbDist based on %d samples; alpha=%.4f>' % (
            self._freqdist.N(), self.alpha)


class DirichletProbDist(nltk.probability.MLEProbDist):

    SUM_TO_ONE = True

    """
        Implements Dirichlet smoothing in the
        NLTK.probability framework.
    """

    def __init__(self, freqdist, background_prob_dist, mu):
        super(DirichletProbDist, self).__init__(freqdist)

        assert isinstance(background_prob_dist, nltk.probability.ProbDistI)
        assert mu > 0.0

        self._background_prob_dist = background_prob_dist
        self.mu = float(mu)

    def prob(self, sample):
        return np.exp(
            np.log(float(self._freqdist.get(sample, 0.0)) +
                   self.mu * self._background_prob_dist.prob(sample)) -
            np.log(float(self._freqdist.N()) + self.mu))

    def __repr__(self):
        return '<DirichletProbDist based on %d samples; mu=%.4f>' % (
            self._freqdist.N(), self.mu)
