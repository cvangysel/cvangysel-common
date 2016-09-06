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

    DOC_PJG = """<DOC>
<DOCNO> FR941110-0-00001 </DOCNO>
<PARENT> FR941110-0-00001 </PARENT>
<TEXT>
 
<!-- PJG FTAG 4700 -->

<!-- PJG STAG 4700 -->

<!-- PJG ITAG l=90 g=1 f=1 -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=90 g=1 f=4 -->
Federal Register
<!-- PJG /ITAG -->

<!-- PJG ITAG l=90 g=1 f=1 -->
&blank;/&blank;Vol. 59, No. 217&blank;/&blank;Thursday, November 10, 1994&blank;/&blank;Rules and Regulations
<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=01 g=1 f=1 -->
Vol. 59, No. 217 
<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=02 g=1 f=1 -->
Thursday, November 10, 1994
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG /STAG -->

<!-- PJG /FTAG -->
</TEXT>
</DOC>



<DOC>
<DOCNO> FR941110-0-00002 </DOCNO>
<PARENT> FR941110-0-00001 </PARENT>
<TEXT>
 
<!-- PJG STAG 4700 -->

<!-- PJG ITAG l=50 g=1 f=1 -->
DEPARTMENT OF AGRICULTURE 
<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=18 g=1 f=1 -->
Agricultural Marketing Service
<!-- PJG /ITAG -->

<!-- PJG ITAG l=52 g=1 f=1 -->

<CFRNO>7 CFR Parts 932 and 944 </CFRNO>
<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=41 g=1 f=1 -->

<RINDOCK>[Docket No. FV94&hyph;932&hyph;1FIR] </RINDOCK>
<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=55 g=1 f=1 -->
Olives Grown in California and Imported Olives; Final Rule Establishing Limited Use Olive Requirements During the
1994&hyph;95 Crop Year 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<AGENCY>
<!-- PJG ITAG l=10 g=1 f=2 -->
AGENCY:
<!-- PJG /ITAG -->

<!-- PJG ITAG l=10 g=1 f=1 -->
 Agricultural Marketing Service, USDA. 
<!-- PJG /ITAG -->

<!-- PJG QTAG 02 -->
<!-- PJG /QTAG -->

<!-- PJG 0012 frnewline -->
</AGENCY>
<ACTION>
<!-- PJG ITAG l=10 g=1 f=2 -->
ACTION:
<!-- PJG /ITAG -->

<!-- PJG ITAG l=10 g=1 f=1 -->
 Final rule. 
<!-- PJG /ITAG -->

<!-- PJG ITAG l=59 g=1 f=1 -->

<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->
</ACTION>
<SUMMARY>
<!-- PJG ITAG l=10 g=1 f=2 -->
SUMMARY:
<!-- PJG /ITAG -->

<!-- PJG ITAG l=10 g=1 f=1 -->
 The Department of Agriculture (Department) is adopting as a final rule without change, the provisions of an interim
final rule which authorized the use of smaller sized olives in the production of limited use styles for California
olives during the 1994&hyph;95 crop year. This rule is intended to allow more olives into fresh market channels and
is consistent with current market demand for olives. As required under section 8e of the Agricultural Marketing Agreement
Act of 1937, this rule also changes the olive import regulation. 
<!-- PJG /ITAG -->

<!-- PJG QTAG 02 -->
<!-- PJG /QTAG -->

<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
</SUMMARY>
<DATE>
<!-- PJG ITAG l=10 g=1 f=2 -->
EFFECTIVE DATE:
<!-- PJG /ITAG -->

<!-- PJG ITAG l=10 g=1 f=1 -->
 December 12, 1994. 
<!-- PJG /ITAG -->

<!-- PJG QTAG 02 -->
<!-- PJG /QTAG -->

<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
</DATE>
<FURTHER>
<!-- PJG ITAG l=10 g=1 f=2 -->
FOR FURTHER INFORMATION CONTACT:
<!-- PJG /ITAG -->

<!-- PJG ITAG l=10 g=1 f=1 -->
 Caroline C. Thorpe, Marketing Order Administration Branch, Fruit and Vegetable Division, AMS, USDA, P.O. Box 96456,
Room 2523&hyph;S, Washington, D.C. 20090&hyph;6456; telephone (202) 720&hyph;5127, or Terry Vawter, California
Marketing Field Office, Fruit and Vegetable Division, AMS, USDA, 2202 Monterey Street, Suite 102&hyph;B, Fresno,
CA 93721, telephone (209) 487&hyph;5901. 
<!-- PJG /ITAG -->

<!-- PJG QTAG 02 -->
<!-- PJG /QTAG -->

<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
</FURTHER>
<SUPPLEM>
<!-- PJG ITAG l=10 g=1 f=2 -->
SUPPLEMENTARY INFORMATION:
<!-- PJG /ITAG -->

<!-- PJG ITAG l=10 g=1 f=1 -->
 This rule is issued under Marketing Agreement No. 148 and Order No. 932 (7 CFR Part 932), as amended, regulating the
handling of olives grown in California, hereinafter referred to as the order. The order is effective under the Agricultural
Marketing Agreement Act of 1937, as amended (7 U.S.C. 601&hyph;674), hereinafter referred to as the Act. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->

<!-- PJG ITAG l=11 g=1 f=1 -->
This rule is also issued under section 8e of the Act, which requires the Secretary of Agriculture to issue grade, size,
quality, or maturity requirements for certain listed commodities, including olives, imported into the United States
that are the same as, or comparable to, those requirements imposed upon the domestic commodities regulated under
the Federal marketing orders. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
The Department is issuing this rule in conformance with Executive Order 12866. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
This rule has been reviewed under Executive Order 12778, Civil Justice Reform. This rule is not intended to have retroactive
effect. This rule will not preempt any State or local laws, regulations, or policies, unless they present an irreconcilable
conflict with this rule. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
The Act provides that administrative proceedings must be exhausted before parties may file suit in court. Under section
608(15)(A) of the Act, any handler subject to an order may file with the Secretary a petition stating that the order,
any provision of the order, or any obligation imposed in connection with the order is not in accordance with law and
requesting a modification of the order or to be exempted therefrom. A handler is afforded the opportunity for a hearing
on the petition. After the hearing the Secretary would rule on the petition. The Act provides that the district court
of the United States in any district in which the handler is an inhabitant, or has his or her principal place of business,
has jurisdiction in equity to review the Secretary's ruling on the petition, provided a bill in equity is filed not
later than 20 days after date of the entry of the ruling. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
There are no administrative procedures which must be exhausted prior to any judicial challenge to the provisions
of import regulations issued under section 8e of the Act. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
Pursuant to the requirements set forth in the Regulatory Flexibility Act (RFA), the Administrator of the Agricultural
Marketing Service (AMS) has considered the economic impact of this action on small entities. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
The purpose of the RFA is to fit regulatory actions to the scale of business subject to such actions in order that small
businesses will not be unduly or disproportionately burdened. Marketing orders issued pursuant to the Act, and rules
issued thereunder, are unique in that they are brought about through group action of essentially small entities acting
on their own behalf. Thus, both statutes have small entity orientation and compatibility. Import regulations issued
under the Act are based on those established under Federal marketing orders. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
There are 5 handlers of California olives that will be subject to regulation under the order during the current season,
and there are about 1,200 olive producers in California. There are approximately 25 importers of olives subject to
the olive import regulation. Small agricultural producers have been defined by the Small Business Administration
(13 CFR 121.601) as those whose annual receipts are less than $500,000, and small agricultural service firms, which
include handlers and importers, have been defined by the Small Business Administration as those having annual receipts
of less than $5,000,000. None of the domestic olive handlers may be classified as small entities. The majority of olive
producers and importers may be classified as small entities. 
<!-- PJG 0012 frnewline -->

<!-- PJG 0012 frnewline -->
Nearly all of the olives grown in the United States are produced in California. The growing areas are scattered throughout
California with most of the commercial production coming from inland valleys. The majority of olives are produced
in central California. California olives are primarily used for canned ripe whole and whole pitted olives which are
eaten out of hand as hors d'oeuvres or used as an ingredient in cooking and in salads. The canned ripe olive market is
essentially a domestic market. A few shipments of California olives are exported. 
<!-- PJG 0012 frnewline -->

<!-- PJG /ITAG -->
</SUPPLEM>
<!-- PJG /STAG -->
</TEXT>
</DOC>"""

    def test_pjg(self):
        ((first_doc_id, first_doc_content),
         (second_doc_id, second_doc_content)) = trec_utils._parse_trectext(
            TRECUtilsTest.DOC_PJG.split('\n'))

    DOC_SPACES = """<DOC>
<DOCNO> LA051289-0030 </DOCNO>
<DOCID> 55902 </DOCID>
<DATE>
<P>
May 12, 1989, Friday, San Diego County Edition
</P>
</DATE>
<SECTION>
<P>
Sports; Part 3; Page 7A; Column 1; Sports Desk
</P>
</SECTION>
<LENGTH>
<P>
595 words
</P>
</LENGTH>
<HEADLINE>
<P>
SOCKERS HOPE LATE FADES WON'T LEAD TO EARLY EXIT FROM PLAYOFFS
</P>
</HEADLINE>
<BYLINE>
<P>
By DON PATTERSON
</P>
</BYLINE>
<DATELINE>
<P>
DALLAS
</P>
</DATELINE>
<TEXT>
<P>
Dig back through a month and a half of Socker games and you can find what
turned out to be the dress rehearsal for Saturday's semifinal playoff loss to
Dallas.
</P>
</TEXT>
</DOC>"""

    def test_spaces(self):
        (doc_id, doc_content), = trec_utils._parse_trectext(
            TRECUtilsTest.DOC_SPACES.split('\n'))

        self.assertEqual(doc_id, 'LA051289-0030')

if __name__ == '__main__':
    unittest.main()
