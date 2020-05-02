from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark_llap import HiveWarehouseSession
import requests
import gzip
import io
import medline as med
import pymysql

base_url = 'http://d1trphadoop01:8088/medline/update'


def list_file_from_mysql():
    db = pymysql.connect("d1drlhadoops01", "medline", "Medline@mda2020", "medline")
    cursor = db.cursor(pymysql.cursors.DictCursor)
    query = "select * from update_status where status is false"
    cursor.execute(query)
    res_list = cursor.fetchall()
    db.close()
    return res_list

def update_mysql():
    db = pymysql.connect("d1drlhadoops01", "medline", "Medline@mda2020", "medline")
    cursor = db.cursor(pymysql.cursors.DictCursor)
    query = "update update_status set status=true"
    cursor.execute(query)
    db.close()

def parse_url_gz_xml(fileobj):
    print(fileobj)
    url = base_url + "/" + fileobj["file_name"]
    r = requests.get(url)
    f = io.BytesIO(r.content)
    return med.parse_medline_xml(gzip.GzipFile(fileobj=f))


if __name__ == '__main__':
    """Process downloaded MEDLINE (served with httpd) to hive database"""
    print("Process MEDLINE file to hive")
    file_list = list_file_from_mysql()
    if len(file_list) == 0:
        print("No file to update yet")
        exit(1)

    spark = SparkSession \
        .builder \
        .appName("medline_xml_parser_update2020") \
        .getOrCreate()
    sc = spark.sparkContext

    hive = HiveWarehouseSession.session(spark).build()
    hive.setDatabase("pubmed")

    file_rdd = sc.parallelize(file_list, numSlices=1)

    parse_results_rdd = file_rdd. \
        flatMap(lambda fileobj: [Row(file_name=fileobj["file_name"], submitted_date=fileobj["update_date"], **publication_dict)
                         for publication_dict in parse_url_gz_xml(fileobj)])

    medline_df = parse_results_rdd.toDF()

    medline_df.select("pmid", "pmc", "doi", "other_id", "title", "abstract", "authors", "affiliations", "mesh_terms",
                      "publication_types", "keywords", "chemical_list", "pubdate", "pubyear", "submitted_date",
                      "journal", "medline_ta", "nlm_unique_id", "issn_linking", "country", "references", "deleteflag",
                      "file_name") \
        .write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector") \
        .mode("append") \
        .option("table", "update2020") \
        .save()

    spark.stop()
