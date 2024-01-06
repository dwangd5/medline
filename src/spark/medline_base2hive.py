from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import requests
from bs4 import BeautifulSoup
import gzip
import io
import medline as med

url = 'http://r1drlhadooprepo:8088/data/medline/baseline'
ext = 'xml.gz'


def listfile(url, ext):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]


def parse_url_gz_xml(url):
    r = requests.get(url)
    if r.status_code == 200:
        f = io.BytesIO(r.content)
        return med.parse_medline_xml(gzip.GzipFile(fileobj=f))
    else:
        # todo: write this into log
        print("-------failed to get file from web server: " + url)


if __name__ == '__main__':
    """Process downloaded MEDLINE (served with httpd) to hive database"""
    print("Process MEDLINE file to hive")

    spark = SparkSession \
        .builder \
        .appName("medline xml parser") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext

    url_rdd = sc.parallelize(listfile(url, ext), numSlices=100)

    parse_results_rdd = url_rdd. \
        flatMap(lambda url: [Row(file_name=os.path.basename(url), **publication_dict) for publication_dict in parse_url_gz_xml(url)])

    medline_df = parse_results_rdd.toDF()

    database_name = "pubmed"
    # note: spark hive session cannot access to managed hive table, so it will create an external table below
    table_name = "base_tmp"

    medline_df.select("pmid", "pmc", "doi", "other_id", "title", "abstract", "authors", "affiliations", "mesh_terms", "publication_types", "keywords", "chemical_list", "pubdate", "pubyear", "journal", "medline_ta", "nlm_unique_id", "issn_linking", "country", "references", "deleteflag")\
        .write \
        .mode("overwrite")\
        .saveAsTable(f"{database_name}.{table_name}")

    spark.stop()
