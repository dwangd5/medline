# medline
## phase 1 development
### medline xml parser
#### environment setup
Python 3.6 is tested. A python virtual environment is preferred in order to clean install and control dependencies. 

Conda is recommended for this purpose. Download and install Anaconda from: 
<a>https://www.anaconda.com/distribution/</a>

<pre>
# conda command to create a new environment
conda create -n medline python=3.6

# activate this virtual env after installation
conda activate medline

# install dependencies that are listed in requirements.txt
# for example
pip install lxml==4.5.0
...
</pre>
Now the environment is ready for the parser

#### simple test script of this parser
You can use the following python script to test the parser functions:
<pre>
# first need to import the src directory to system path so that the medline package can be imported
# change the src path to your local path
import sys
sys.path.append("/Users/dwangd5/Project/github/dwangd5/medline/src")
import pprint
import medline as med

# change the data directory to your local path
list = med.parse_medline_xml('data/pubmed20n0014.xml.gz')
pprint.pprint(list)
</pre>

You should be able to see the following schema: 
<pre>
[...,
 {'abstract': 'Three cases with postinflammatory inner ear sequelae are '
              'presented to illustrate unusual histopathologic changes. '
              'Endolymphatic hydrops without changes in the perilymphatic '
              'system was present in one ear following "influenza" meningitis '
              'and labyrinthitis ossificans in the contralateral ear. The '
              'characteristic histopathological changes of the temporal bones '
              'with hematogenic bacterial infection were an extensive '
              'labyrinthine ossification associated with a generalized '
              'sclerotic change of the whole periotic bone. Bony fixation of '
              'the stapedial footplate occurred with the generalized '
              'inflammatory process of the otic capsule. Severe and diffuse '
              'labyrinthitis ossificans occurred in one case due to '
              'tympanogenic inflammation spreading through the round window '
              'membrane in the course of suppurative otitis media. A general '
              'immunosuppression leading to fatal termination was the apparent '
              'factor predisposing to the inner ear complication.',
  'affiliations': '',
  'authors': 'F Suga;JR Lindsay',
  'chemical_list': '',
  'country': 'United States',
  'delete': False,
  'doi': '10.1177/000348947708600105',
  'issn_linking': '0003-4894',
  'journal': 'The Annals of otology, rhinology, and laryngology',
  'keywords': '',
  'medline_ta': 'Ann Otol Rhinol Laryngol',
  'mesh_terms': 'D000328:Adult; D002648:Child; D003051:Cochlea; D007758:Ear, '
                'Inner; D004432:Ear, Middle; D005260:Female; D006801:Humans; '
                'D007251:Influenza, Human; D007759:Labyrinth Diseases; '
                'D007762:Labyrinthitis; D008297:Male; D008587:Meningitis, '
                'Viral; D008875:Middle Aged; D009999:Ossification, '
                'Heterotopic; D010019:Osteomyelitis; D010033:Otitis Media; '
                'D013701:Temporal Bone',
  'nlm_unique_id': '0407300',
  'other_id': '',
  'pmc': '',
  'pmid': '402099',
  'pubdate': '1977',
  'publication_types': 'D002363:Case Reports; D016428:Journal Article; '
                       "D013487:Research Support, U.S. Gov't, P.H.S.",
  'pubyear': 1977,
  'references': '',
  'title': 'Labyrinthitis ossificans.'},
...
</pre>