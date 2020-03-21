# medline
## phase 1 development: parse medline xml.gz
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

Note: medline original parser (medline -> parser.py file) is borrowed from the following github repository:

https://github.com/titipata/pubmed_parser

Based on above, some customization were added.   

#### simple test script of this parser
You can use the following python script to test the parser functions:
<pre>
# in src folder, execute the following in medline conda virtual environment created above:
python tests/test.py
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

## phase 2 development: load 30 million medline records into data store

### make medline parser as library to be referenced by pyspark:
zip src->medline folder as medline.zip, and submit this zip file with --py-files together with pyspark.  

### pyspark running in local: put into mysql
MySQL is not recommended due to large amount of records. 

pyspark program to parse medline xml.gz files into dataframe, and then write to mysql. 

this is how to run it:

```shell script
# cd src/conf, and cp config.json.template to config.json, modify the contents to local env
# in src folder, execute the following in medline conda virtual environment created above:
source activate medline
spark-submit --conf spark.driver.host=localhost --master local[2] \
--py-files <path_to_medline.zip\> \
--packages mysql:mysql-connector-java:8.0.19 <path_to_medline_base2mysql.py>
```
You can modify the code to upload updates as well. 

### pyspark running in cloudera hadoop cluster (HDP)
note: this procedure was tested with HDP 3.1.4. 

<pre>
# Steps to parse medline xml.gz and put in hive database
1. run doc->schema->hive.ddl to create database and tables (base, update2020)
2. download medline base and update, and host in httpd web server. 
medline base and update must be distributed to cluster nodes in order to run pyspark on cluster. 
three options: 
1) put in local file system on all cluster nodes. This is bad because distribute 30G data to all servers is not good idea. 
2) upload medline to hdfs. this is optimal, however, this requires add additional functions to break kerberos, and also re-write lxml functions to read file from hdfs. 
due to time limitation, I will push this for later. 
3) put medline files in a web server. this is not ideal, but it is workable. 
however, please fine tune your httpd web server so that it can deal at least several hundred concurrency. 

I used option 3 for now. httpd fine tunning is very important for smooth running of pyspark. 

3. run pyspark on HDP cluster as follows:
spark-submit --master yarn --deploy-mode client \
--num-executors 20 --executor-memory 10g --executor-cores 2 \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" \
--conf spark.sql.hive.hiveserver2.jdbc.url.principal=hive/_HOST@REALM \
--conf spark.hadoop.hive.llap.daemon.service.hosts=@llap0 \
--conf spark.hadoop.hive.zookeeper.quorum=zookeeper1:2181,zookeeper2:2181,zookeeper3:2181 \
--jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.1.4.0-315.jar \
--py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.4.0-315.zip,"path_to medline.zip" \
"path_to medline_base2hive.py" 

Note: 
1) change zookeeper, jar, zip files, python files to your HDP environment.
2) Pls limit number of executors, executor-cores to the capacity of your httpd server can handle concurrency.

4. run similar pyspark job to medline update files using script: medline_update2hive.py

5. update2020 table has files to delete. merge "base + update2020 - delete" into a single table: medline
Note: 
1) you can refer to doc -> schema -> hive.dml for the merge sql statement.
2) I run this merge using beeline, and have beeline heap size as 1G, hive tez container as 4g.  

</pre>

## phase 3 development: UMLS encoding of medline subject and abstract
coming soon. 