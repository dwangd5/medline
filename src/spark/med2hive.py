import sys
sys.path.append(".")
import os
from glob import glob
import medline as med
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark_llap import HiveWarehouseSession

if __name__ == '__main__':
    """Process downloaded MEDLINE folder to mysql database"""
    print("Process MEDLINE file to mysql")

    spark = SparkSession \
        .builder \
        .appName("medline xml parser") \
        .getOrCreate()
    sc = spark.sparkContext

    hive = HiveWarehouseSession.session(spark).build()
    hive.setDatabase("medline")

    path_rdd = sc.parallelize(glob("/tmp/medline/test/*.xml.gz"), numSlices=10)

    # print("-----------------" + str(path_rdd.count()))

    parse_results_rdd = path_rdd. \
        flatMap(lambda x: [Row(file_name=os.path.basename(x), **publication_dict)
                           for publication_dict in med.parse_medline_xml(x)])
    # print("-----------------" + str(parse_results_rdd.count()))
    medline_df = parse_results_rdd.toDF()

    medline_df.select("pmid", "pmc", "doi", "other_id", "title", "abstract", "authors", "mesh_terms", "publication_types", "keywords", "chemical_list", "pubdate", "pubyear", "journal", "medline_ta", "nlm_unique_id", "issn_linking", "country", "references", "deleteflag")\
        .write.format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")\
        .mode("overwrite")\
        .option("table", "articles")\
        .save()

    sc.stop()
