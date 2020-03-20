import sys
sys.path.append(".")
import os
from glob import glob
import medline as med
from pyspark.sql import SparkSession
from pyspark.sql import Row
from conf import config

config_path = "conf/config.json"
config_set = config.read(config_path)

# directory
download_dir = config_set.get("medline").get("baseDir")
numSlices = config_set.get("medline").get("numSlices")

if __name__ == '__main__':
    """Process downloaded MEDLINE folder to mysql database"""
    print("Process MEDLINE file to mysql")

    print(config_set.get("medline"))

    spark = SparkSession \
        .builder \
        .appName("medline xml parser") \
        .getOrCreate()
    sc = spark.sparkContext

    path_rdd = sc.parallelize(glob(os.path.join(download_dir, '*.xml.gz')), numSlices=numSlices)

    print("-----------------" + download_dir)
    print("-----------------" + str(path_rdd.count()))

    parse_results_rdd = path_rdd. \
        flatMap(lambda x: [Row(file_name=os.path.basename(x), **publication_dict)
                           for publication_dict in med.parse_medline_xml(x)])
    print("-----------------" + str(parse_results_rdd.count()))
    medline_df = parse_results_rdd.toDF()

    medline_df.write.format("jdbc").options(
        url=config_set.get("mysql").get("url"),
        driver=config_set.get("mysql").get("driver"),
        dbtable=config_set.get("mysql").get("dbtable"),
        user=config_set.get("mysql").get("user"),
        password=config_set.get("mysql").get("password")).mode('overwrite').save()
    spark.stop()
