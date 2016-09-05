import unittest

from cvangysel import io_utils


class IOUtilsTest(unittest.TestCase):

    def test_windowed_translated_token_stream(self):
        words = {
            '</s>': io_utils.Word(id=0, count=1),
            'world': io_utils.Word(id=1, count=1),
            'foo': io_utils.Word(id=2, count=1),
            'bar': io_utils.Word(id=3, count=1),
            'hello': io_utils.Word(id=4, count=1),
        }

        text = ('hello', 'world', 'world',
                'hello', 'bar', 'hello',
                'foo', 'world', 'foo',
                'foo', 'world', 'bar')

        stream = io_utils.windowed_translated_token_stream(
            iter(text),
            window_size=3,
            words=words)

        self.assertEqual(
            list(stream),
            [(4, 1, 1), (1, 1, 4), (1, 4, 3), (4, 3, 4),
             (3, 4, 2), (4, 2, 1), (2, 1, 2), (1, 2, 2),
             (2, 2, 1), (2, 1, 3)])

    def test_windowed_translated_token_stream_2stride(self):
        words = {
            '</s>': io_utils.Word(id=0, count=1),
            'world': io_utils.Word(id=1, count=1),
            'foo': io_utils.Word(id=2, count=1),
            'bar': io_utils.Word(id=3, count=1),
            'hello': io_utils.Word(id=4, count=1),
        }

        text = ('hello', 'world', 'world',
                'hello', 'bar', 'hello',
                'foo', 'world', 'foo',
                'foo', 'world', 'bar')

        stream = io_utils.windowed_translated_token_stream(
            iter(text),
            window_size=3,
            words=words,
            stride=2)

        self.assertEqual(
            list(stream),
            [(4, 1, 1), (1, 4, 3), (3, 4, 2), (2, 1, 2), (2, 2, 1)])

    def test_windowed_translated_token_stream_3stride(self):
        words = {
            '</s>': io_utils.Word(id=0, count=1),
            'world': io_utils.Word(id=1, count=1),
            'foo': io_utils.Word(id=2, count=1),
            'bar': io_utils.Word(id=3, count=1),
            'hello': io_utils.Word(id=4, count=1),
        }

        text = ('hello', 'world', 'world',
                'hello', 'bar', 'hello',
                'foo', 'world', 'foo',
                'foo', 'world', 'bar')

        stream = io_utils.windowed_translated_token_stream(
            iter(text),
            window_size=3,
            words=words,
            stride=3)

        self.assertEqual(
            list(stream),
            [(4, 1, 1), (4, 3, 4), (2, 1, 2), (2, 1, 3)])

    def test_windowed_translated_token_stream_1skip(self):
        words = {
            '</s>': io_utils.Word(id=0, count=1),
            'world': io_utils.Word(id=1, count=1),
            'foo': io_utils.Word(id=2, count=1),
            'bar': io_utils.Word(id=3, count=1),
            'hello': io_utils.Word(id=4, count=1),
        }

        text = ('hello', 'world', 'world',
                'hello', 'bar', 'hello',
                'foo', 'world', 'foo',
                'foo', 'world', 'bar')

        stream = io_utils.windowed_translated_token_stream(
            iter(text),
            window_size=3,
            words=words,
            skips=(1,))

        world, foo, bar, hello = range(1, 5)

        self.assertEqual(
            list(stream),
            [(hello, world, bar), (world, hello, hello),
             (world, bar, foo), (hello, hello, world),
             (bar, foo, foo), (hello, world, foo),
             (foo, foo, world), (world, foo, bar)])

    def test_windowed_translated_token_stream_0and1skip(self):
        words = {
            '</s>': io_utils.Word(id=0, count=1),
            'world': io_utils.Word(id=1, count=1),
            'foo': io_utils.Word(id=2, count=1),
            'bar': io_utils.Word(id=3, count=1),
            'hello': io_utils.Word(id=4, count=1),
        }

        text = ('hello', 'world', 'world',
                'hello', 'bar', 'hello',
                'foo', 'world', 'foo',
                'foo', 'world', 'bar')

        stream = io_utils.windowed_translated_token_stream(
            iter(text),
            window_size=3,
            words=words,
            skips=(0, 1,))

        world, foo, bar, hello = range(1, 5)

        self.assertEqual(
            sorted(list(stream)),
            sorted([
                (hello, world, world), (world, world, hello),
                (world, hello, bar), (hello, bar, hello),
                (bar, hello, foo), (hello, foo, world),
                (foo, world, foo), (world, foo, foo),
                (foo, foo, world), (foo, world, bar),
                (hello, world, bar), (world, hello, hello),
                (world, bar, foo), (hello, hello, world),
                (bar, foo, foo), (hello, world, foo),
                (foo, foo, world), (world, foo, bar)]))

    def test_windowed_translated_token_stream_bigrams_trigrams(self):
        words = {
            '</s>': io_utils.Word(id=0, count=1),
            'world': io_utils.Word(id=1, count=1),
            'foo': io_utils.Word(id=2, count=1),
            'bar': io_utils.Word(id=3, count=1),
            'hello': io_utils.Word(id=4, count=1),
        }

        text = ('hello', 'world', 'world',
                'hello', 'bar', 'hello',
                'foo', 'world', 'foo',
                'foo', 'world', 'bar')

        stream = io_utils.windowed_translated_token_stream(
            iter(text),
            window_size=(2, 3),
            words=words)

        world, foo, bar, hello = range(1, 5)

        self.assertEqual(
            sorted(list(stream)),
            sorted([
                (hello, world), (world, world), (world, hello),
                (hello, bar), (bar, hello), (hello, foo),
                (foo, world), (world, foo), (foo, foo),
                (foo, world), (world, bar),
                (4, 1, 1), (1, 1, 4), (1, 4, 3), (4, 3, 4),
                (3, 4, 2), (4, 2, 1), (2, 1, 2), (1, 2, 2),
                (2, 2, 1), (2, 1, 3)]))

if __name__ == '__main__':
    unittest.main()
