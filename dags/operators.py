import logging
import boto3
import re
import requests
import boto
import os
import json
import time
# import httplib
import tools
import datetime
import configparser

from builtins import bytes
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile
from boto3.session import Session
from boto.s3.key import Key
from jinja2 import Template
from pymongo import MongoClient
from access import DruidAccess, S3Access, Task
from dateutil import parser

from airflow.models import BaseOperator, Variable
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.utils.file import TemporaryDirectory
from airflow.utils.email import send_email


def create_s3_path(dag, extra=""):
    if extra and extra[0] != "/":
        extra = "/" + extra
    return "airflow/dag_" + dag.dag_id + extra


def translate_bucket_name(bucket):
    env = Variable.get("environment")
    if env == "staging":
        return bucket + "-staging"
    elif env == "production":
        return bucket
    raise ValueError("Bad 'environment' variable. Supported values are 'staging' and 'production'.")


class AwsSqsHook(BaseHook):

    def __init__(self, sqs_conn_id="sqs_default"):
        super(AwsSqsHook, self).__init__(sqs_conn_id)
        self.sqs_conn_id = sqs_conn_id

    def get_sqs_client(self):
        conn = self.get_connection(self.sqs_conn_id)
        session = Session(aws_access_key_id=conn.login,
                          aws_secret_access_key=conn.password, region_name="us-east-1")
        return session.client('sqs')

    def get_queue_attributes(self, queue):
        sqs = self.get_sqs_client()
        q_url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
        return sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=['All'])["Attributes"]


class _SparkMaster:
    """
    Basic routines for handling Spark clusters
    """
    def __init__(self, cluster_id, tmp_dir, env):
        self._env = env
        self._tmp_dir = tmp_dir
        self.cluster_id = cluster_id

    def wait_for_finish(self):
        if not self.cluster_id:
            raise ValueError("cluster_id must be set before waiting for task to finish")
        last_state = "WAITING"
        start_time = time.time()
        while True:
            data = self.run_and_get_json(
                ["/usr/local/bin/aws", "emr", "describe-cluster", "--cluster-id", self.cluster_id])
            state = data["Cluster"]["Status"]["State"]
            if state in ["STARTING", "BOOTSTRAPPING", "RUNNING", "TERMINATING", "SHUTTING_DOWN"]:
                logging.info("State is `%s`.. still waiting" % state)
            elif state in ["COMPLETED", "TERMINATED"] or (state == "WAITING" and last_state != state):
                logging.info("Task completed succesfully with state " + state)
                return
            elif state in ["WAITING"]:
                logging.info("Waiting for the cluster to start up")
                if time.time() - start_time > 10 * 60:
                    logging.info("Task failed on WAITING timeout")
                    raise AirflowException(
                        "Spark job failed, waiting timeout reached")
            else:
                logging.info(
                    "Task failed on status `%s`. Full cluster info was `%r`" % (state, data))
                raise AirflowException("Spark job failed")
            time.sleep(30)
            last_state = state

    def verify_cluster_is_ready(self):
        if not self.cluster_id:
            raise ValueError("cluster_id must be set before waiting for task to finish")
        data = self.run_and_get_json(
            ["/usr/local/bin/aws", "emr", "describe-cluster", "--cluster-id", self.cluster_id])
        state = data["Cluster"]["Status"]["State"]
        if state != "WAITING":
            logging.info(
                "Spark cluster is not ready. Task failed on status `%s`. Full cluster info was `%r`" % (state, data))
            raise AirflowException("Spark job failed - Cluster was not ready")
        logging.info("Cluster is ready")

    def run_and_get_json(self, cmd):
        result = None
        while not result:
            sp = Popen(cmd, stdout=PIPE, stderr=STDOUT, cwd=self._tmp_dir, env=self._env)
            final_out = ''
            for line in iter(sp.stdout.readline, b''):
                line = line.decode('utf-8').strip()
                final_out += line
            sp.wait()
            if sp.returncode:
                logging.info(
                    "Command exited with return code {0}".format(sp.returncode))
                logging.info("Output: " + final_out)
                # Sleep a while and try again if we get throttled
                if "ThrottlingException" in final_out:
                    time.sleep(10)
                else:
                    raise AirflowException("Spark run failed")
            else:
                result = final_out
        return json.loads(result)


class GenericHook(BaseHook):

    def __init__(self, conn_id="generic_default"):
        super(GenericHook, self).__init__(conn_id)
        self.conn_id = conn_id

    def get_credentials(self):
        conn = self.get_connection(self.conn_id)
        return conn.login, conn.password

    def get_raw_connection(self):
        return self.get_connection(self.conn_id)


class ZabbixOperator(BaseOperator):
    """
    Send monitoring information to Zabbix

    Requires `zabbix_monitor` variable to be set

    Will send provided monitoring data to Zabbix. If no extra params are provided will
    send a generic 'alive' message.
    """
    template_fields = ('key', 'value', 'hostname',)
    ui_color = '#E0F2FB'

    @apply_defaults
    def __init__(
            self, key=None, value="1", hostname="airflow",
            *args, **kwargs):
        if not key:
            key = "dags." + kwargs["dag"].dag_id + ".alive"
        kwargs["task_id"] = "zabbix_" + key.replace(".", "_")
        self.key = key
        self.value = value
        self.hostname = hostname
        super(ZabbixOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        tools.zabbix_report(self.key, self.value, self.hostname)


class S3CleanerOperator(BaseOperator):
    """
    Operator for cleaning up files in S3. As default locations needs to follow strict formatting. Free-form
    deleting can be enabled by setting isolation to False.
    """

    template_fields = ('s3_path', 'bucket', 's3_conn_id')
    ui_color = '#E2553F'

    @apply_defaults
    def __init__(
            self, s3_conn_id, bucket, s3_path="", isolation=True,
            *args, **kwargs):
        super(S3CleanerOperator, self).__init__(*args, **kwargs)
        self.bucket = translate_bucket_name(bucket)
        self.s3_path = s3_path
        if isolation:
            self.s3_path = create_s3_path(self.dag, s3_path)
        self.s3_conn_id = s3_conn_id
        self.isolation = isolation

    def execute(self, context):
        if not self.isolation:
            raise AirflowException("Forced isolation is in effect, this can be removed if needed but should be done with caution.")
        key, secret = GenericHook(self.s3_conn_id).get_credentials()
        s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret)
        logging.info("Cleaning up " + self.s3_path)
        objects = s3.list_objects(Bucket=self.bucket, Prefix=self.s3_path).get("Contents")
        if objects:
            for key in objects:
                k = key.get("Key")
                if k[-1] == "/":
                    logging.info("Skipping base path")
                    continue
                logging.info("Deleting " + key.get("Key"))
                s3.delete_object(Bucket=self.bucket, Key=key.get("Key"))


# class StlShortCircuitOperator(BaseOperator):
#     """
#     Stop-The-Line short circuit operator.

#     Requires `etcd` variable to be set.

#     Will fail and stop the execution if any service has set the global stop-the-line flag.
#     """
#     template_fields = ('key',)
#     ui_color = '#FFE6F2'

#     @apply_defaults
#     def __init__(
#             self, key="/v2/keys/analytics/all_ok", task_id='continue_if_pipeline_ok',
#             *args, **kwargs):
#         kwargs["task_id"] = task_id
#         super(StlShortCircuitOperator, self).__init__(*args, **kwargs)
#         self.key = key

#     def execute(self, context):
#         for i in range(5):
#             try:
#                 logging.info("Checking stop the line status..")
#                 if self.check_stop_the_line():
#                     return
#             except Exception, e:
#                 logging.info("Exception when getting status %s" % e)
#         raise AirflowException("Stop the line flag is enabled")

#     def check_stop_the_line(self):
#         mon = Variable.get("etcd")
#         if "disabled" in mon:
#             return True
#         conn = httplib.HTTPConnection(mon)
#         conn.request('GET', self.key)
#         resp = conn.getresponse()
#         content = json.loads(resp.read())
#         return content["node"]["value"] == "OK"


class HttpGetStoreToS3Operator(BaseOperator):
    """
    HTTP GET and store to S3 operator.

    Will make an HTTP GET request to the given URL and store the response body to S3 .

    By default it overwrites the file in S3 if it already exists. Set overwrite=false in args to avoid that.
    In overwrite is disabled and file already exists, task instance will fail.

    If you have request params you want to pass, define them as a dictionay called params_dict inside operators params.
    For example: params={'params_dict':your_dictionary}
    """
    template_fields = ('url', 's3bucket', 's3folder', 's3_file_name')
    ui_color = '#FFE6F2'

    @apply_defaults
    def __init__(
            self, url, s3_file_name, s3bucket, s3folder, s3_conn_id='s3_default', prep_func=None, provide_context=False,
            overwrite=False, auth_pair=None, op_kwargs=None, headers=None, verify=True, *args, **kwargs):
        super(HttpGetStoreToS3Operator, self).__init__(*args, **kwargs)
        self.url = url
        self.prep_func = prep_func
        self.s3bucket = s3bucket
        self.s3folder = s3folder
        self.s3_conn_id = s3_conn_id
        self.s3_file_name = s3_file_name
        self.provide_context = provide_context
        self.op_kwargs = op_kwargs or {}
        self.auth_pair = auth_pair or ('temp_user', 'temp_pwd')
        self.headers = headers or {}
        self.verify = verify
        self.overwrite_flag = overwrite

    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
        if self.prep_func:
            self.prep_func(context)
        res_body = self.make_request_get_response(context)
        self.store_result_to_s3(res_body)

    def make_request_get_response(self, context):
        param_str = "&".join("%s=%s" % (k, v)
                             for k, v in context['params']['params_dict'].items())
        req = requests.get(self.url, params=param_str, auth=self.auth_pair,
                           headers=self.headers, verify=self.verify)
        logging.info("url= %r" % req.url)
        if req.status_code != requests.codes.ok:
            raise AirflowException("HTTP GET failed: %r. Error message: %r" % (
                req.status_code, req.content))
        res_body = req.content
        return res_body

    def store_result_to_s3(self, content):
        key, secret = GenericHook(self.s3_conn_id).get_credentials()
        s3 = boto.connect_s3(key, secret)
        bucket = s3.get_bucket(self.s3bucket)
        file_path = self.s3folder + self.s3_file_name
        file_key = bucket.get_key(file_path)
        if not file_key:
            file_key = Key(bucket)
            file_key.key = file_path
        elif not self.overwrite_flag:
            raise AirflowException("File already exists in S3!")
        file_key.set_contents_from_string(content)


class FeatureFlagShortCircuitOperator(ShortCircuitOperator):
    """
    Feature-flag short circuit operator.

    Will fail and stop the execution if variable value is anythign but 'on'. Skips everything if
    'off' and otherwise fail.
    """
    template_fields = ('variable',)
    ui_color = '#FFE6F2'

    @apply_defaults
    def __init__(
            self, variable, *args, **kwargs):
        kwargs["task_id"] = "continue_if_" + variable
        kwargs["python_callable"] = self.check_validity
        super(FeatureFlagShortCircuitOperator, self).__init__(*args, **kwargs)
        self.variable = variable

    def check_validity(self):
        v = Variable.get(self.variable).lower().strip()
        if v in ["on", "true", "1", "yes", "y", "t"]:
            return True
        elif v in ["off", "false", "0", "no", "n", "f"]:
            return False
        raise AirflowException(
            "Bad value for feature flag. Has to be either 'on' or 'off'")


class PostgresXComSingleOperator(BaseOperator):
    """ Execute SQL and push first rows first value to XCom """
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#D1FBC0'

    @apply_defaults
    def __init__(
            self, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(PostgresXComSingleOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        return self.hook.get_records(self.sql, parameters=self.parameters)[0][0]


class CopyFromS3ToLocal(BaseOperator):
    """ Copy stuff from S3 to local disk """
    template_fields = ('s3_from', 'local_dir')
    ui_color = '#B763E1'

    @apply_defaults
    def __init__(
            self,
            s3_from,
            local_dir,
            overwrite=False,
            s3_conn_id='s3_default',
            *args, **kwargs):
        super(CopyFromS3ToLocal, self).__init__(*args, **kwargs)
        self.s3_from = s3_from
        self.local_dir = local_dir
        self.overwrite = overwrite
        self.s3_conn_id = s3_conn_id

    def download(self):
        key, secret = GenericHook(self.s3_conn_id).get_credentials()
        s3 = boto3.client('s3', aws_access_key_id=key,
                          aws_secret_access_key=secret)
        bucket = translate_bucket_name(re.findall("s3://([a-zA-Z\-]*)/", self.s3_from)[0])
        key_base = re.findall("s3://[a-zA-Z\-]*/(.*)", self.s3_from)[0]
        keys = []
        logging.info("Listing %s (%s:%s)", self.s3_from, bucket, key_base)
        objects = s3.list_objects(
            Bucket=bucket, Prefix=key_base).get("Contents")
        if objects:
            for ft in objects:
                keys.append(ft.get("Key"))
            for key in keys:
                if key[-1] == "/":
                    logging.info("Skipping base path")
                    continue
                logging.info("Found %r, downloading", key)
                filename = key.split("/")[-1]
                s3.download_file(bucket, key, self.local_dir +
                                 os.path.sep + filename)
        else:
            logging.info("Nothing found to copy %s (%s:%s)",
                         self.s3_from, bucket, key_base)

    def execute(self, context):
        # Dummy solution for now
        if self.overwrite:
            self.download()
        else:
            logging.info("Overwrite is disabled, not downloading anything")


class S3ToRedshift(BaseOperator):
    """ COPY data from S3 to Redshift """

    template = """
    COPY {{ table }} FROM '{{ s3_from }}' WITH
    credentials AS 'aws_access_key_id={{ aws_key }};aws_secret_access_key={{ aws_secret }}'
    {% for cmd in extra_commands %} {{ cmd }}{% endfor %}
    """

    template_fields = ('table', 's3_from', 'sql')
    template_ext = ('.sql',)
    ui_color = '#E3C62C'

    @apply_defaults
    def __init__(
            self,
            table,
            s3_from,
            postgres_conn_id='postgres_default',
            s3_conn_id='s3_default',
            extra_commands=[],
            dummy=False,
            parameters=None,
            *args, **kwargs):
        super(S3ToRedshift, self).__init__(*args, **kwargs)
        self.table = table
        bucket = translate_bucket_name(re.findall("s3://([a-zA-Z\-]*)/", s3_from)[0])
        key_base = re.findall("s3://[a-zA-Z\-]*/(.*)", s3_from)[0]
        self.s3_from = "s3://" + bucket + "/" + key_base
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.parameters = parameters
        self.extra_commands = extra_commands
        self.dummy = dummy
        self.sql = Template(self.template).render(table=self.table, s3_from=self.s3_from,
                                                  extra_commands=self.extra_commands,
                                                  aws_key="#" * 10, aws_secret="#" * 10)

    def execute(self, context):
        self.ps_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        login, password = GenericHook(self.s3_conn_id).get_credentials()
        sql = Template(self.template).render(table=self.table, s3_from=self.s3_from,
                                             extra_commands=self.extra_commands,
                                             aws_key=login, aws_secret=password)
        # Do not log the sql as it contains the actual secrets, use self.sql
        # instead
        logging.info("Executing: " + self.sql)
        if not self.dummy:
            self._run(sql, parameters=self.parameters)
        else:
            logging.info("Dummy flag is on, skipping actual execution")

    def _run(self, sql, autocommit=True, parameters=None):
        """ Copy of the run in PostgresHook without logging """
        conn = self.ps_hook.get_conn()
        if isinstance(sql, basestring):
            sql = [sql]

        if self.ps_hook.supports_autocommit:
            self.ps_hook.set_autocommit(conn, autocommit)

        cur = conn.cursor()
        for s in sql:
            if parameters is not None:
                cur.execute(s, parameters)
            else:
                cur.execute(s)
        cur.close()
        conn.commit()
        conn.close()


class RedshiftToS3(BaseOperator):
    """ UNLOAD from Redshift to S3 """

    template = """
    UNLOAD
    ('{{ sql }}')
    TO
    '{{ s3_to }}'
    credentials AS 'aws_access_key_id={{ aws_key }};aws_secret_access_key={{ aws_secret }}'
    {% for cmd in extra_commands %} {{ cmd }}{% endfor %}
    """

    template_fields = ('s3_to', 'sql', 'load_sql')
    template_ext = ('.sql',)
    ui_color = '#FBDA34'

    @apply_defaults
    def __init__(
            self,
            load_sql,
            s3_to,
            postgres_conn_id='postgres_default',
            s3_conn_id='s3_default',
            extra_commands=[],
            autocommit=False,
            parameters=None,
            dummy=False,
            *args, **kwargs):
        super(RedshiftToS3, self).__init__(*args, **kwargs)
        bucket = translate_bucket_name(re.findall("s3://([a-zA-Z\-]*)/", s3_to)[0])
        key_base = re.findall("s3://[a-zA-Z\-]*/(.*)", s3_to)[0]
        self.s3_to = "s3://" + bucket + "/" + key_base
        # Escape possible single quotes
        self.load_sql = load_sql.replace("'", "\\'")
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.extra_commands = extra_commands
        self.dummy = dummy
        self.sql = Template(self.template).render(sql=self.load_sql, s3_to=self.s3_to,
                                                  extra_commands=self.extra_commands,
                                                  aws_key="#" * 10, aws_secret="#" * 10)

    def execute(self, context):
        self.ps_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        login, password = GenericHook(self.s3_conn_id).get_credentials()
        sql = Template(self.template).render(sql=self.load_sql, s3_to=self.s3_to,
                                             extra_commands=self.extra_commands,
                                             aws_key=login, aws_secret=password)
        # Do not log the sql as it contains the actual secrets, use self.sql
        # instead
        logging.info("Executing: " + self.sql)
        if not self.dummy:
            self._run(sql, parameters=self.parameters)
        else:
            logging.info("Dummy flag is on, skipping actual execution")

    def _run(self, sql, autocommit=False, parameters=None):
        """ Copy of the run in PostgresHook without logging """
        conn = self.ps_hook.get_conn()
        if isinstance(sql, basestring):
            sql = [sql]

        if self.ps_hook.supports_autocommit:
            self.ps_hook.set_autocommit(conn, autocommit)

        cur = conn.cursor()
        for s in sql:
            if parameters is not None:
                cur.execute(s, parameters)
            else:
                cur.execute(s)
        cur.close()
        conn.commit()
        conn.close()


class LongLifeSparkOperator(BaseOperator):
    """ Operator for running jobs on Spark clusters already available """

    template = [
        "/usr/local/bin/aws", "emr", "add-steps",
        "--cluster-id", "{{ cluster_id }}",
        "--steps", "{{ steps }}"]
    template_fields = ('cluster_id', 'steps', 'cmd')
    ui_color = '#D0FBD7'

    @apply_defaults
    def __init__(
            self,
            cluster_id,
            steps,
            aws_conn_id="s3_app_mapreduce",
            pool="aws_spark_pool",
            *args, **kwargs):
        super(LongLifeSparkOperator, self).__init__(pool=pool, *args, **kwargs)
        key, secret = GenericHook(aws_conn_id).get_credentials()
        self._env = {"AWS_DEFAULT_REGION": "us-east-1",
                     "AWS_ACCESS_KEY_ID": key,
                     "AWS_SECRET_ACCESS_KEY": secret}
        self.env = {"AWS_DEFAULT_REGION": "us-east-1",
                    "AWS_ACCESS_KEY_ID": "#" * 10,
                    "AWS_SECRET_ACCESS_KEY": "#" * 10}
        self.steps = steps
        self.cluster_id = cluster_id
        cmd = [Template(t).render(
            dag_id=self.dag_id, steps=self.steps, cluster_id=self.cluster_id) for t in self.template]
        self.cmd = " ".join(cmd)


    def execute(self, context):
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            master = _SparkMaster(self.cluster_id, tmp_dir, self._env)
            cmd = [Template(t).render(dag_id=self.dag_id, steps=self.steps, cluster_id=self.cluster_id) for t in self.template]
            logging.info("Checking status of the cluster %s" % self.cluster_id)
            master.verify_cluster_is_ready()
            logging.info("Running command: " + " ".join(cmd))
            master.run_and_get_json(cmd)
            logging.info("Job is running. Waiting for execution to finish.")
            master.wait_for_finish()


class RedshiftVacuumOperator(BaseOperator):
    """ Executes a vacuum """

    template_fields = ('sql', 'table')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, table,
            postgres_conn_id='postgres_default',
            parameters=None,
            amount=95,
            extra="",
            *args, **kwargs):
        """
        :type table: str
        :type postgres_conn_id: str
        :type amount: int
        """
        kwargs["task_id"] = kwargs["task_id"] if kwargs.get(
            "task_id") else "vacuum_" + table
        kwargs["pool"] = kwargs["pool"] if kwargs.get(
            "pool") else postgres_conn_id + "_vacuum_pool"
        super(RedshiftVacuumOperator, self).__init__(*args, **kwargs)
        self.parameters = parameters
        self.table = table
        self.sql = "VACUUM " + extra + " " + table
        if amount:
            self.sql += " TO " + str(amount) + " PERCENT"
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.hook.supports_autocommit = True
        self.hook.run(self.sql, True, parameters=self.parameters)


class SparkOperator(BaseOperator):
    """ Run a Spark Job """
    key, secret = GenericHook("s3_app_mapreduce").get_credentials()
    s3 = boto3.resource('s3', aws_access_key_id=key, aws_secret_access_key=secret)
    src_bucket = s3.Bucket("applifier-mapreduce")
    src_key_name = "airflow/config/config.ini"
    obj = src_bucket.Object(src_key_name)
    with open('config.ini', 'wb') as data:
        obj.download_fileobj(data)

    config = configparser.ConfigParser()
    current_dir = os.getcwd()
    config_file_absolute_path = current_dir + '/' + 'config.ini'
    config.read(config_file_absolute_path)
    environment = tools.get_env()

    template = [
        "/usr/local/bin/aws", "emr", "create-cluster",
        "--name", "airflow_{{ dag_id }}",
        "--release-label", "{{ release_label }}",
        "--instance-type", "{{ instance_type }}",
        "--instance-count", "{{ instance_count }}",
        "--log-uri", config.get(environment, "Logs"),
        "--ec2-attributes", config.get(environment, "KeyPair"),
        "--use-default-roles",
        "--applications", "Name=Spark",
        "--bootstrap-action", config.get(environment, "PrepEnv"),
        "--steps", "{{ steps }}",
        "--auto-terminate"]
    template_fields = ('steps', 'cmd', 'instance_type', '_cmd')
    ui_color = '#D0FBD7'

    @apply_defaults
    def __init__(
            self,
            steps,
            release_label="emr-4.5.0",
            instance_type="m3.xlarge",
            instance_count=4,
            aws_conn_id="s3_app_mapreduce",
            subnet=None,
            pool="aws_spark_pool",
            config=None,
            *args, **kwargs):
        super(SparkOperator, self).__init__(pool=pool, *args, **kwargs)
        key, secret = GenericHook(aws_conn_id).get_credentials()
        self._env = {"AWS_DEFAULT_REGION": "us-east-1",
                     "AWS_ACCESS_KEY_ID": key,
                     "AWS_SECRET_ACCESS_KEY": secret}
        self.env = {"AWS_DEFAULT_REGION": "us-east-1",
                    "AWS_ACCESS_KEY_ID": "#" * 10,
                    "AWS_SECRET_ACCESS_KEY": "#" * 10}
        self.steps = steps
        if subnet is None:
            subnet = self.config.get(self.environment, "Subnet")
        self.subnet = subnet
        self.release_label = release_label
        self.instance_count = instance_count
        self.instance_type = instance_type
        self._cmd = [Template(t).render(dag_id=self.dag_id, steps=self.steps, subnet=self.subnet, release_label=self.release_label,
                                        instance_type=self.instance_type, instance_count=self.instance_count) for t in self.template]
        # This is a bad hack to get optional configurations for the cluster
        if config is not None:
            conf_file_name = "dag_%s_conf.json" % self.dag_id
            config_file = os.path.join(".", conf_file_name)
            logging.info("config_file= %r" % os.path.abspath(config_file))
            with open(config_file, "w") as handle:
                handle.write(config)
            logging.info("stored file. exists? %r, %r" % (os.path.isfile(config_file), os.path.isfile("file://%s" % os.path.abspath(config_file))))
            self._cmd.append("--configurations")
            self._cmd.append("file://%s" % os.path.abspath(config_file)) # self._cmd.append(config_file)
        self.cmd = " ".join(self._cmd)

    def execute(self, context):
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            master = _SparkMaster(None, tmp_dir, self._env)
            logging.info("Running command: " + self.cmd)
            result = master.run_and_get_json(self._cmd)
            cluster_id = result["ClusterId"]
            master.cluster_id = cluster_id
            logging.info("Cluster `%s` has been created. Waiting for execution to finish." % cluster_id)
            master.wait_for_finish()


class BashScriptOperator(BaseOperator):
    """
    Exact copy of original Airflow operator. Runs the given bash command, returns the output is xcom_push is set to True.
    """

    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            prep_func=None,
            *args, **kwargs):
        """
        If xcom_push is True, the last line written to stdout will also
        be pushed to an XCom when the bash command completes.
        """
        super(BashScriptOperator, self).__init__(*args, **kwargs)
        self.bash_command = Template(bash_command).render(dag_id=self.dag_id)
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        self.prep_func = prep_func

    def execute(self, context):
        # Call the prep function first if there's any
        if self.prep_func:
            self.prep_func(context)
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        bash_command = self.bash_command
        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))
                logging.info("Running command: ")
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env)

                self.sp = sp

                logging.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    logging.info(line)
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        logging.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()


class MongoOperator(BaseOperator):
    """
    Mongo operator.

    Will run a given js function in Mongo using pymnongo's eval() function.
    If param send_email is true, will send the result as an email to the given email addresses.
    """
    template_fields = ()
    ui_color = '#FFE6F2'

    @apply_defaults
    def __init__(
            self, mongo_script, script_params=None, send_email=False, email=None, email_subject=None,
             results_as_attachment=False, *args, **kwargs):
        super(MongoOperator, self).__init__(*args, **kwargs)
        self.mongo_script = mongo_script
        self.script_params = script_params
        self.send_email = send_email
        self.email = email
        self.email_subject = email_subject
        self.results_as_attachment = results_as_attachment

    def execute(self, context):
        client = MongoClient(Variable.get("m2d_mongo"))
        db = client['comet']
        result = db.eval(self.mongo_script, self.script_params).encode('utf-8')

        if self.send_email:
            if self.results_as_attachment:
                file_name = self.email_subject
                with open(file_name, 'wb') as f:
                    f.write(result)
                send_email(to=self.email, subject=self.email_subject, html_content='', files=[file_name])
                os.remove(file_name)
            else:
                send_email(to=self.email, subject=self.email_subject, html_content=result)


class CountS3PrefixSensor(BaseOperator):
    '''
    Similar to S3PrefixSensor, but waits for exact N prefix to exist.

    :param s3_conn_id: Name of the S3 connection id
    :type s3_conn_id: str

    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str

    :param prefix_func: Function to return the prefix string.
        The function should take a `context` object as input and return a str
    :type prefix_func: Python callable

    :param target_count: Number of prefix to expect
    :type target_count: int

    :param delimiter: The delimiter intended to show hierarchy.
        Defaults to '/'.
    :type delimiter: str

     :param poke_interval: Time in seconds that the job should wait in between each tries
    :type poke_interval: int

    '''

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 bucket_name,
                 prefix_func,
                 target_count,
                 delimiter='/',
                 poke_interval=datetime.timedelta(hours=1).total_seconds(),
                 timeout=datetime.timedelta(days=1).total_seconds(),
                 *args, **kwargs):

        self.s3_conn_id = s3_conn_id
        self.bucket_name = bucket_name
        self.prefix_func = prefix_func
        self.target_count = target_count
        self.delimiter = delimiter

        super(CountS3PrefixSensor, self).__init__(
            email_on_retry=False,
            retries=int(timeout/poke_interval),
            retry_delay=datetime.timedelta(seconds=poke_interval),
            retry_exponential_backoff=False,
            *args, **kwargs)

    def execute(self, context):
        aws_access_key_id, aws_secret_access_key = GenericHook(self.s3_conn_id).get_credentials()
        s3_connection = boto.connect_s3(
            aws_access_key_id, aws_secret_access_key, host="s3.amazonaws.com")
        prefix = self.prefix_func(context)
        logging.info('Checking prefix: %s', prefix)
        folders = list(s3_connection.get_bucket(self.bucket_name)
                       .list(prefix=prefix, delimiter=self.delimiter))
        logging.info('Folders:\n%s', '\n'.join([folder.name for folder in folders]))
        if len(folders) != self.target_count:
            raise AirflowException('Sensor retry. Waiting for %d, got %d' % (self.target_count, len(folders)))


class S3ToRedshiftWithTruncate(S3ToRedshift):
    """ COPY data from S3 to Redshift With Truncate """

    template_no_truncate = """
    COPY {{ table }} FROM '{{ s3_from }}' WITH
    credentials AS 'aws_access_key_id={{ aws_key }};aws_secret_access_key={{ aws_secret }}'
    {% for cmd in extra_commands %} {{ cmd }}{% endfor %}
    """

    template_truncate = """
    begin transaction;
    truncate table {{table}};
    COPY {{ table }} FROM '{{ s3_from }}' WITH
    credentials AS 'aws_access_key_id={{ aws_key }};aws_secret_access_key={{ aws_secret }}'
    {% for cmd in extra_commands %} {{ cmd }}{% endfor %};
    commit;
    """


    template_fields = ('table', 's3_from', 'sql')
    template_ext = ('.sql',)
    ui_color = '#E3C62C'

    @apply_defaults
    def __init__(
            self,
            table,
            s3_from,
            postgres_conn_id='postgres_default',
            s3_conn_id='s3_default',
            extra_commands=[],
            dummy=False,
            parameters=None,
            truncate=False,
            *args, **kwargs):
        super(S3ToRedshift, self).__init__(*args, **kwargs)
        self.table = table
        bucket = translate_bucket_name(re.findall("s3://([a-zA-Z\-]*)/", s3_from)[0])
        key_base = re.findall("s3://[a-zA-Z\-]*/(.*)", s3_from)[0]
        self.s3_from = "s3://" + bucket + "/" + key_base
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.parameters = parameters
        self.extra_commands = extra_commands
        self.dummy = dummy

        if truncate == False:
            self.template = self.template_no_truncate
        else:
            self.template = self.template_truncate
        self.sql = Template(self.template).render(table=self.table, s3_from=self.s3_from,
                                                  extra_commands=self.extra_commands,
                                                  aws_key="#" * 10, aws_secret="#" * 10)


class S3ToDruidBatchIngestionOperator(BaseOperator):
    """
    S3ToDruid operator - It is written based on S3 path structure that Secor Service uses for Unity Ads In
    Both staging and Production Environment.
    This is used to transfer data from S3 to Druid
    Supports both DAILY and HOURLY roll ups
    topic=Kafka Topic which persisted to S3 and will be send to Druid.
    template=Druid Ingestion Json file on S3
    data_source = Druid data source
    aggregate = DAILY OR HOURLY
    log_date = date for which data needs to be ingested
    log_hour = hour for which data needs to be ingested. Required for hourly ingestion.

    """
    @apply_defaults
    def __init__(self, s3_bucket, prefix, s3_conn_id, data_source, slots,
                 template, topic, folder=None, log_date_fun=None, log_hour_fun=None,  log_hour="00", aggregate="DAILY", date_key="date", hour_key="hour", provide_context=False, op_kwargs=None, *args, **kwargs):
        super(S3ToDruidBatchIngestionOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.prefix = prefix
        self.s3_conn_id = s3_conn_id
        self.data_source = data_source
        self.slots = slots
        self.template = template
        self.date_key = date_key
        self.hour_key = hour_key
        self.topic = topic
        self.folder = folder
        self.aggregate = aggregate
        self.log_date_fun = log_date_fun
        self.log_hour_fun=log_hour_fun
        self.log_hour = log_hour
        self.provide_context = provide_context
        self.op_kwargs = op_kwargs or {}

    def execute(self, context):

        if self.provide_context:
            context.update(self.op_kwargs)

        log_date = context['ti'].execution_date.strftime('%Y-%m-%d')
        if self.log_date_fun is not None:
            log_date = self.log_date_fun(context)
        if self.log_hour_fun is not None:
            self.log_hour = self.log_hour_fun(context)

        current_dir = os.getcwd()
        s3_bucket_with_env = tools.s3_name_generator(self.s3_bucket, "-json", "-staging")
        s3_conn_id_with_env = tools.s3_name_generator(self.s3_conn_id, "_json", "_staging")
        s3_key_path = "druid-json-template" + "/" + self.template
        self.download_template_file(s3_conn_id_with_env, s3_bucket_with_env, s3_key_path, current_dir + "/json")

        druid_host, druid_port = Variable.get("druid_overlord").split(":")
        key, secret = GenericHook(s3_conn_id_with_env).get_credentials()
        druid = DruidAccess(druid_host, druid_port, "", "", self.data_source)
        s3 = S3Access(key, secret, False)

        if self.folder is None:
            self.folder = tools.s3_name_generator(self.prefix, "-prod", "-staging")

        s3_file_location = ""
        if self.aggregate == "DAILY":
            s3_file_location = "s3://{bucket}/{folder}/{topic}/{day_key}={log_date}/".format(
                bucket=s3_bucket_with_env, folder=self.folder,
                topic=self.topic, day_key=self.date_key,
                log_date=log_date)
        if self.aggregate == "HOURLY":
            s3_file_location = "s3://{bucket}/{folder}/{topic}/{day_key}={log_date}/{hour_key}={log_hour}/".format(
                bucket=s3_bucket_with_env, folder=self.folder,
                topic=self.topic, day_key=self.date_key,
                log_date=log_date, hour_key=self.hour_key, log_hour=self.log_hour)

        logging.info("Launching importer for %s.." % s3_file_location)
        s3_files = s3.get_filenames(s3_file_location)
        logging.info("Files Name " + ','.join(s3_files))
        running_tasks = []
        s3_files = ['"' + f + '"' for f in sorted(s3_files)]
        task = Task(s3_files)
        log_timestamp = log_date + ' ' + self.log_hour + ":00:00"
        log_timestamp_ts = (parser.parse(log_timestamp)).isoformat()
        next_log_timestamp_ts = (parser.parse(log_timestamp) + datetime.timedelta(days=1)).isoformat()
        if self.aggregate == "HOURLY":
            next_log_timestamp_ts = (parser.parse(log_timestamp) + datetime.timedelta(hours=1)).isoformat()

        logging.info("Handling task %r" % task)
        task.id = druid.upload(log_timestamp_ts, next_log_timestamp_ts, s3_files, self.template)
        logging.info("Uploading task to druid and task id is %r" % task.id)
        running_tasks.append(task)
        # Cleaning and waiting
        druid.clean_tasks(running_tasks)
        while len(running_tasks) >= self.slots:
            logging.info("Waiting for %r tasks" % len(running_tasks))
            time.sleep(10)
            running_tasks = druid.clean_tasks(running_tasks)
        while len(running_tasks) > 0:
            logging.info("Waiting for finalization of %r tasks" % len(running_tasks))
            time.sleep(10)
            running_tasks = druid.clean_tasks(running_tasks)
        logging.info("Importing done..")


    def download_template_file(self, s3_conn_id, bucket, prefix, local_dir):
        s3_from = "s3://" + bucket + "/" + prefix
        if not os.path.exists(local_dir):
            os.mkdir(local_dir)
        key, secret = GenericHook(s3_conn_id).get_credentials()
        s3 = boto3.client('s3', aws_access_key_id=key, aws_secret_access_key=secret)
        keys = []
        logging.info("Listing %s (%s:%s)", s3_from, bucket, prefix)
        objects = s3.list_objects(Bucket=bucket, Prefix=prefix).get("Contents")
        if objects:
            for ft in objects:
                keys.append(ft.get("Key"))
            for key in keys:
                if key[-1] == "/":
                    logging.info("Skipping base path")
                    continue
                logging.info("Found %r, downloading", key)
                filename = key.split("/")[-1]
                local_file = os.path.join(local_dir, filename)
                s3.download_file(bucket, key, local_file)
                logging.info("Put template file into dir %r", local_file)
                os.chmod(local_file, 777)
        else:
            logging.info("Nothing found to copy %s (%s:%s)", s3_from, bucket, prefix)


class RedshiftSchemaMigrationOperator(BaseOperator):
    """
    This should check the table to see if all columns exist.  If not, create the missing columns.For sample use case refer to sample test cases under tests folder.
    Inputs:
    schema: Schema of Redshift table (ex: schema = "reporting")
    table : Redshift table name
    required_columns : list of tuples (string,string,string,bool) (ex: [("col1", "varchar(40)", "lzo", False),("col3", "bool","lzo", False)])
    postgres_conn_id : postgres connection id
    postgres_conn_pool : postgres connection pool
    """

    ### QUERIES

    QUERY_SCHEMA_TEMPLATE = """
        SET SEARCH_PATH TO {schema};
        SELECT "column", type, encoding, "notnull"
        FROM pg_table_def WHERE schemaname = '{schema}' AND tablename = '{table}';
    """

    # query that add new columns to the input table

    ALTER_TABLE_TEMPLATE = """
        ALTER TABLE {table} ADD COLUMN {column_name} {column_type} ENCODE {encoding} {not_null};
    """

    @apply_defaults
    def __init__(self,schema, table, required_columns, postgres_conn_id,postgres_conn_pool, *args, **kwargs):
        self.schema=schema
        self.table = table
        self.required_columns = required_columns
        self.postgres_conn_id = postgres_conn_id
        self.postgres_conn_pool = postgres_conn_pool
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, pool=self.postgres_conn_pool)
        super(RedshiftSchemaMigrationOperator, self).__init__(*args, **kwargs)

    def validate_required_columns(self):
        if self.required_columns is not None:
            for column in self.required_columns:
                if len(column) != 4:
                    raise ValueError('Required columns is a list of tuple(column_name,column_type,encoding,not null)')

    def build_table_schema_query(self):
        return self.QUERY_SCHEMA_TEMPLATE.format(schema=self.schema, table=self.table)

    def build_add_column_query(self,column_name,column_type,encoding,not_null):
        return  self.ALTER_TABLE_TEMPLATE.format(table=self.table, column_name=column_name, column_type=column_type, encoding=encoding, not_null=not_null)

    # Transaction query will be an atomic operation.

    def build_transaction_query(self,new_columns):
        transaction_query="BEGIN TRANSACTION; "
        for column_name, column_type, encoding, not_null_boolean in new_columns:
            is_null = "not null"
            if not not_null_boolean:
                is_null = "null"
            query = self.build_add_column_query(column_name,column_type,encoding,is_null)
            transaction_query = transaction_query + ' ' + query
        transaction_query=transaction_query + ' ' + "COMMIT;"
        return transaction_query

    def get_current_columns(self,query,hook):
        current_columns_rows = hook.get_pandas_df(query).values
        current_columns = []
        for row in current_columns_rows:
            current_columns.append(tuple(row))
        return current_columns

    """
    Finding new columns which needs to be added

    Input: List of existing column names
    Return List of (column_name,type,encoding,not null) representing new columns to be added 
    """
    def find_new_columns(self, existing_columns):
        if(self.required_columns == None):
            return []
        existing_columns_keys = set([x[0] for x in existing_columns])
        new_columns = filter(lambda x: x[0] not in existing_columns_keys, self.required_columns)
        if not new_columns:
            new_columns = []
        return new_columns

    def execute_query(self, query):
        conn = self.hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.commit()
        conn.close()

    def execute(self, context):
        self.validate_required_columns()
        query=self.build_table_schema_query()
        current_columns = self.get_current_columns(query,self.hook)
        new_columns = self.find_new_columns(current_columns)
        add_new_columns_query = self.build_transaction_query(new_columns)
        self.execute_query(add_new_columns_query)


class SqsPersistOperator(BaseOperator):

    @apply_defaults
    def __init__(self, sqs_name, s3_bucket_name, s3_conn_id, sqs_conn_id, sqs_persist_path,
                 max_num_of_message_persist_at_once=1, file_name_fun=None, region_name = 'us-east-1', *args, **kwargs):
        super(SqsPersistOperator, self).__init__(*args, **kwargs)
        self.sqs_name = tools.s3_name_generator(sqs_name, "-production", "-staging")
        self.s3_bucket_name = tools.s3_name_generator(s3_bucket_name, "-json", "-staging")
        self.s3_conn_id = tools.s3_name_generator(s3_conn_id, "_json", "_staging")
        self.sqs_conn_id = sqs_conn_id
        self.sqs_persist_path = sqs_persist_path
        self.file_name_fun = file_name_fun
        self.max_num_of_message_persist_at_once = max_num_of_message_persist_at_once
        self.region_name = region_name

    def new_message_in_sqs(self, sqs_client):
        queue = sqs_client.get_queue_by_name(QueueName=self.sqs_name)
        logging.info("Found queue : " + queue.url)
        messages = queue.receive_messages(AttributeNames=['All'],
                                          MaxNumberOfMessages=self.max_num_of_message_persist_at_once)

        if len(messages) > 0:
            return True, messages
        else:
            return False, None

    def persist_message_in_s3(self, context, message, s3_access):
        if self.file_name_fun is None:
            file_name = "sqs-persist-message-" + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S.%f') + ".txt"
        else:
            file_name = self.file_name_fun(context)

        sqs_persist_key = self.sqs_persist_path + '/' + file_name
        logging.info("Uploading " + sqs_persist_key + " to s3")
        s3_access.upload_file(bucket_name=self.s3_bucket_name, key=sqs_persist_key, data=message.body)

    def execute(self, context):
        key, secret = GenericHook(self.sqs_conn_id).get_credentials()
        sqs_client = boto3.resource("sqs", aws_access_key_id=key,
                                    aws_secret_access_key=secret,
                                    region_name=self.region_name)

        key, secret = GenericHook(self.s3_conn_id).get_credentials()
        s3_access = S3Access(key, secret, False)

        is_new_message_in_sqs, messages = self.new_message_in_sqs(sqs_client)
        task_instance = context['task_instance']

        if is_new_message_in_sqs:
            for message in messages:
                logging.info("There is new message in the queue waiting to be processed, message body " + message.body)
                self.persist_message_in_s3(context, message, s3_access)
                message.delete()
            task_instance.xcom_push(key='new_message_in_sqs', value=True)
        else:
            logging.info("There is no message in sqs " )
            task_instance.xcom_push(key='new_message_in_sqs', value=False)
