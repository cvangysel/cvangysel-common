import unittest

from cvangysel import trec_utils, io_utils


class TRECUtilsTest(unittest.TestCase):

    DOC = """<DOC>
<DOCNO>FT922-5992</DOCNO>
<PROFILE>_AN-CE1BRAAAFT</PROFILE>
<DATE>920527
</DATE>
<HEADLINE>
FT  27 MAY 92 / World News In Brief: No second term
</HEADLINE>
<TEXT>
Russian president Boris Yeltsin said he did not plan to run for a second
term in 1996.
</TEXT>
<PUB>The Financial Times
</PUB>
<PAGE>
International Page 1
</PAGE>
</DOC>"""

    def test_trecweb(self):
        (doc_id, content), = trec_utils._parse_trectext(
            TRECUtilsTest.DOC.split('\n'))

        self.assertEqual(
            content,
            ['<PROFILE>_AN-CE1BRAAAFT</PROFILE>',
             '<DATE>920527',
             '</DATE>',
             '<HEADLINE>',
             'FT  27 MAY 92 / World News In Brief: No second term',
             '</HEADLINE>',
             '<TEXT>',
             'Russian president Boris Yeltsin said '
             'he did not plan to run for a second',
             'term in 1996.',
             '</TEXT>',
             '<PUB>The Financial Times',
             '</PUB>',
             '<PAGE>',
             'International Page 1',
             '</PAGE>'])

if __name__ == '__main__':
    unittest.main()
