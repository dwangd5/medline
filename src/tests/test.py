import sys
sys.path.append(".")
import pprint
import medline as med

list = med.parse_medline_xml('data/pubmed20n0014.xml.gz')
pprint.pprint(list)
