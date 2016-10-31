import logging
import sys
import traceback

import multiprocessing
import queue


class WorkerFunction(object):
    """
        Decorator for multiprocessing worker functions which protects
        against failures raised within workers.

        Usage (top-level of your module):
            def worker_fn_(payload):
                pass

            worker_fn = multiprocessing_utils.WorkerFunction(worker_fn_)

        Afterwards, pass worker_fn to a multiprocessing.Pool instance.
    """

    def __init__(self, f):
        self.f = f

    def __call__(self, *args, **kwargs):
        try:
            return self.f(*args, **kwargs)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()

            logging.error(
                'Exception occured within worker: %s (%s); %s',
                exc_value, exc_type, traceback.format_tb(exc_traceback))

            raise


class WorkerMetaclass(type):

    """
        Metaclass which encapsulates multiprocessing in a
        convenient functor-style paradigm.

        Usage:
            from cvangysel import multiprocessing_utils

            import numpy as np

            class PowerFn(object,
                          metaclass=multiprocessing_utils.WorkerMetaclass):

                @staticmethod
                def worker(x):
                    return np.power(x, PowerFn.exponent)

            if __name__ == "__main__":
                # The exponent keyword argument is passed to every worker and
                # is accessible within each worker as PowerFn.exponent.
                power_fn = PowerFn(processes=32, exponent=2)

                # The following loop processes the iterable over 32 processes.
                for squared in power_fn(range(1000)):
                    print(squared)
    """

    @staticmethod
    def pool_initializer(cls, kwds):
        for key, value in kwds.items():
            setattr(cls.__class__, key, value)

        setattr(cls.__class__, 'process', multiprocessing.current_process())

    def clazz_call(self, iterable):
        if self.pool:
            return self.pool.imap_unordered(
                WorkerFunction(self.worker), iterable)
        else:
            return (self.worker(payload) for payload in iterable)

    def __init__(cls, name, bases, dct):
        assert hasattr(cls, 'worker'), \
            '{} should implement worker function.'.format(cls)

        cls.__call__ = WorkerMetaclass.clazz_call

        super(WorkerMetaclass, cls).__init__(name, bases, dct)

    def __call__(self, processes, **kwargs):
        clazz = super(WorkerMetaclass, self).__call__()

        if processes > 1:
            pool = multiprocessing.Pool(
                processes=processes,
                initializer=WorkerMetaclass.pool_initializer,
                initargs=(clazz, kwargs))
        else:
            pool = None

            WorkerMetaclass.pool_initializer(clazz, kwargs)

        clazz.pool = pool

        return clazz


class QueueIterator(object):

    def __init__(self, pool, result_object, queue):
        self.pool = pool
        self.result_object = result_object
        self.queue = queue

        self.finished = False

        self.count = 0

    def __next__(self):
        while True:
            if self.result_object.ready() and not self.finished:
                logging.debug('All workers finished.')

                assert self.result_object.successful()

                worker_results = self.result_object.get()
                logging.debug('Retrieved results from workers: %s',
                              worker_results)

                if all(isinstance(result, int) for result in worker_results):
                    self.expected_number_items = sum(worker_results)

                self.finished = True

            queue_finished = self.queue.empty()

            # Safe-guarding code.
            if queue_finished and hasattr(self, 'expected_number_items'):
                logging.debug('Expected %d items; encountered %d.',
                              self.expected_number_items, self.count)

                if self.count > self.expected_number_items:
                    logging.error('Received more objects than expected.')

                    raise RuntimeError()

                queue_finished = (
                    queue_finished and
                    self.expected_number_items == self.count)

            if self.finished and queue_finished:
                logging.debug('Queue is empty.')

                self.pool.terminate()
                logging.debug('Pool terminated.')

                self.pool.join()
                logging.debug('Joined process pool thread.')

                self.queue.close()
                logging.debug('Result queue closed.')

                self.queue.join_thread()
                logging.debug('Joined result queue thread.')

                raise StopIteration()

            try:
                result = self.queue.get(block=False)
                self.count += 1

                return result
            except queue.Empty:
                continue
