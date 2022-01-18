from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from pyhive import hive
from krbcontext.context import krbContext

# MySqlOperator doesn't return value to xcom, so write a wrapper to return
class ReturningMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        return hook.get_records(
            self.sql,
            parameters=self.parameters)

default_args = {'owner': 'dwang7', 'start_date': days_ago(1), 'email': ['dwang7@mdanderson.org'], 'email_on_failure': True}

dag = DAG(dag_id='medline_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 14 * * *")

task_download_file = SSHOperator(
    task_id="download_file",
    command="source activate medline; cd /root/pubmed; python download.py -d /var/www/html/medline/update",
    ssh_conn_id="ssh_r1drlhadooprepo",
    do_xcom_push=False,
    dag=dag)

task_check_new_file = ReturningMySqlOperator (
    task_id="check_new_file",
    sql="select count(*) from medline.update_status where status=0",
    mysql_conn_id="mysql_gate",
    dag=dag)

def branch_func(ti, **kwargs):
    result = ti.xcom_pull(task_ids='check_new_file')
    num_files = result[0][0]
    if num_files > 0:
        return "update2hive"
    else:
        return "end"

task_branch = BranchPythonOperator (
    task_id="branch",
    python_callable= branch_func,
    dag=dag)

task_update2hive = SparkSubmitOperator(
    task_id='update2hive',
    conn_id='spark_default',
    env_vars={
        "PATH": "/opt/anaconda3/envs/medline/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin",
        "PYSPARK_PYTHON": "/opt/anaconda3/envs/medline/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/opt/anaconda3/envs/medline/bin/python"
    },
    application='hdfs:///tmp/airflow/medline_update2hive_2021.py',
    keytab="/opt/airflow/keytabs/smokeuser.headless.keytab",
    principal="ambari-qa-hornet_dev@MDANDERSON.EDU",
    conf={
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/opt/anaconda3/envs/medline/bin/python",
        "spark.sql.hive.hiveserver2.jdbc.url": "jdbc:hive2://d1drlhadoop03.mdanderson.edu:2181,d1drlhadoop04.mdanderson.edu:2181,d1drlhadoop05.mdanderson.edu:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive",
        "spark.sql.hive.hiveserver2.jdbc.url.principal": "hive/_HOST@MDANDERSON.EDU",
        "spark.hadoop.hive.llap.daemon.service.hosts": "@llap0",
        "spark.hadoop.hive.zookeeper.quorum": "d1drlhadoop03.mdanderson.edu:2181,d1drlhadoop04.mdanderson.edu:2181,d1drlhadoop05.mdanderson.edu:2181",
        "spark.jars": "/usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.1.4.0-315.jar",
        "spark.submit.pyFiles": "/usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.4.0-315.zip,hdfs:///tmp/airflow/medline.zip",
        "spark.executor.heartbeatInterval": "50s"},
    num_executors=10,
    executor_cores=5,
    executor_memory='20g',
    driver_memory='4g',
    name='airflow_medline_update2hive',
    dag=dag,
)

task_update_mysql_metadata = MySqlOperator(
    task_id='update_mysql_metadata',
    mysql_conn_id='mysql_gate',
    sql=r"""update medline.update_status set status=1""",
    dag=dag
)

task_mysql_metadata_on_failure = DummyOperator(
    task_id='mysql_metadata_on_failure',
    trigger_rule="one_failed",
    dag=dag
)

def update_covid_subset():
    with krbContext(using_keytab=True, principal='ambari-qa-hornet_dev@MDANDERSON.EDU', keytab_file='/opt/airflow/keytabs/smokeuser.headless.keytab'):
        connection = hive.connect(host="d1drlhadoop06", \
                                      port=10000, \
                                      database="pubmed", \
                                      username="dwang7", \
                                      auth="KERBEROS", \
                                      kerberos_service_name="hive")
        cur = connection.cursor()
        query1 = "ALTER MATERIALIZED VIEW pubmed.update2021_non_repeats REBUILD"
        cur.execute(query1)
        query2 = "ALTER MATERIALIZED VIEW pubmed.covid19_subset_with_repeats REBUILD"
        cur.execute(query2)
        query3 = "ALTER MATERIALIZED VIEW pubmed.covid19_subset REBUILD"
        cur.execute(query3)

task_update_materialized_view_update2021 = PythonOperator (
    task_id="update_materialized_view_update2021",
    python_callable=update_covid_subset,
    dag=dag
)


task_nlp_profiling = SparkSubmitOperator(
    task_id='nlp_profiling',
    conn_id='spark_default',
    env_vars={
        "PATH": "/opt/anaconda3/envs/medline/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin",
        "PYSPARK_PYTHON": "/opt/anaconda3/envs/medline/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/opt/anaconda3/envs/medline/bin/python"
    },
    application='hdfs:///tmp/airflow/spark-1.0-standalone.jar',
    java_class="com.solution.stresstest.spark.ProcessText2Hive",
    application_args=["--source-sql", "\"select pmid, concat(title, ' ' , abstract) as text from pubmed.covid19_subset where pmid not in (select id from pubmed.covid19_subset_profiling)\"", "--property-file", "/opt/public_mm_lite/config/metamaplite.cluster.covid19.properties", "--target-database", "pubmed", "--target-table", "covid19_subset_profiling", "--partition", "100"],
    keytab="/opt/airflow/keytabs/smokeuser.headless.keytab",
    principal="ambari-qa-hornet_dev@MDANDERSON.EDU",
    conf={
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/opt/anaconda3/envs/medline/bin/python",
        "spark.sql.hive.hiveserver2.jdbc.url": "jdbc:hive2://d1drlhadoop03.mdanderson.edu:2181,d1drlhadoop04.mdanderson.edu:2181,d1drlhadoop05.mdanderson.edu:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive",
        "spark.sql.hive.hiveserver2.jdbc.url.principal": "hive/_HOST@MDANDERSON.EDU",
        "spark.hadoop.hive.llap.daemon.service.hosts": "@llap0",
        "spark.hadoop.hive.zookeeper.quorum": "d1drlhadoop03.mdanderson.edu:2181,d1drlhadoop04.mdanderson.edu:2181,d1drlhadoop05.mdanderson.edu:2181",
        "spark.jars": "/usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.1.4.0-315.jar,hdfs:///tmp/airflow/spark-1.0-standalone.jar",
        "spark.executor.heartbeatInterval": "50s"},
    num_executors=20,
    executor_cores=5,
    executor_memory='10g',
    driver_memory='4g',
    name='airflow_medline_nlp_profiling',
    dag=dag,
)

task_nlp_concept_mapping = SparkSubmitOperator(
    task_id='nlp_concept_mapping',
    conn_id='spark_default',
    env_vars={
        "PATH": "/opt/anaconda3/envs/medline/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin",
        "PYSPARK_PYTHON": "/opt/anaconda3/envs/medline/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/opt/anaconda3/envs/medline/bin/python"
    },
    application='hdfs:///tmp/airflow/spark-1.0-standalone.jar',
    java_class="com.solution.stresstest.spark.ProcessConceptMatchedWord2Hive",
    application_args=["--source-sql", "\"select pmid, concat(title, ' ' , abstract) as text from pubmed.covid19_subset where pmid not in (select pmid from pubmed.covid19_subset_concept_mapping)\"", "--property-file", "/opt/public_mm_lite/config/metamaplite.cluster.covid19.properties", "--target-database", "pubmed", "--target-table", "covid19_subset_concept_mapping", "--partition", "100"],
    keytab="/opt/airflow/keytabs/smokeuser.headless.keytab",
    principal="ambari-qa-hornet_dev@MDANDERSON.EDU",
    conf={
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/opt/anaconda3/envs/medline/bin/python",
        "spark.sql.hive.hiveserver2.jdbc.url": "jdbc:hive2://d1drlhadoop03.mdanderson.edu:2181,d1drlhadoop04.mdanderson.edu:2181,d1drlhadoop05.mdanderson.edu:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive",
        "spark.sql.hive.hiveserver2.jdbc.url.principal": "hive/_HOST@MDANDERSON.EDU",
        "spark.hadoop.hive.llap.daemon.service.hosts": "@llap0",
        "spark.hadoop.hive.zookeeper.quorum": "d1drlhadoop03.mdanderson.edu:2181,d1drlhadoop04.mdanderson.edu:2181,d1drlhadoop05.mdanderson.edu:2181",
        "spark.jars": "/usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.1.4.0-315.jar,hdfs:///tmp/airflow/spark-1.0-standalone.jar",
        "spark.executor.heartbeatInterval": "50s"},
    num_executors=20,
    executor_cores=5,
    executor_memory='10g',
    driver_memory='4g',
    name='airflow_medline_nlp_concept_mapping',
    dag=dag,
)

task_end = DummyOperator(
    task_id='end',
    dag=dag,
)

task_download_file >> task_check_new_file >> task_branch >> [task_update2hive, task_end]
task_update2hive >> task_update_mysql_metadata >> [task_update_materialized_view_update2021, task_mysql_metadata_on_failure]
task_mysql_metadata_on_failure >> task_end
task_update_materialized_view_update2021 >> task_nlp_profiling >> task_nlp_concept_mapping >> task_end