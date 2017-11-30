import boto3
import json
import re
import requests
import logging
import jinja2

class DruidAccess:

    URI_QUERY = "http://%s/druid/v2"
    URI_INDEXER = "http://%s/druid/indexer/v1/task"
    URI_STATUS = "http://%s/druid/indexer/v1/task/%s/status"

    def __init__(self, overlord_host, overlord_port, broker_host, broker_port, data_source):
        self._overlord = "%s:%s" % (overlord_host, str(overlord_port))
        self._broker = "%s:%s" % (broker_host, str(broker_port))
        self.data_source = data_source

    def upload(self, from_ts, to_ts, urls, template):
        params = {'ts_from': from_ts,
                  'ts_to': to_ts,
                  'data_source': self.data_source,
                  's3_uris': "[" + ",".join(urls) + "]",
                  's3n_uris': ",".join(urls).replace("s3", "s3n").replace("\"", "")}
        query_json = self.render_template_from_package(template, params)
        logging.info("Uploading Druid task: %s" % query_json)
        url = self.URI_INDEXER % self._overlord
        headers = {'Content-Type': 'application/json'}
        res = requests.post(url, headers=headers, data=query_json)
        if not res.ok:
            raise StandardError(
                "Uploading task to Druid failed because: %s. Query JSON was: %r" % (res.reason, query_json))
        return json.loads(res.text)['task']

    def render_template_from_package(self, filename, params):
        path = 'json/'
        return jinja2.Environment(loader=jinja2.FileSystemLoader(path)).get_template(filename).render(params)

    def clean_tasks(self, task_list):
        new_tasks = []
        for task in task_list:
            task.status = self.status(task.id)["status"]
            if task.status == "SUCCESS":
                logging.debug("Task %s is done.. cleaning up files" % (task.id))
            elif task.status == "RUNNING":
                logging.debug("Task %s still running.." % (task.id))
                new_tasks.append(task)
            else:
                logging.warn("Task %s failed with status: %s" % (task.id, task.status))
                raise StandardError("Failed to process")
        return new_tasks

    def status(self, task_id):
        url = self.URI_STATUS % (self._overlord, task_id)
        try:
            res = requests.get(url)
            status = json.loads(res.text)['status']
        except Exception as e:
            logging.warn("Problem querying task status")
            return "error"
        return status

class S3Access:

    def __init__(self, key, secret, delete_files):
        self._log = logging.getLogger(__name__)
        self._read_only = not delete_files
        self._key = key
        self._secret = secret
        self._s3 = boto3.client(
            's3', aws_access_key_id=self._key, aws_secret_access_key=self._secret)
        self._s3r = boto3.resource(
            's3', aws_access_key_id=self._key, aws_secret_access_key=self._secret)

    def get_filenames(self, base_path):
        keys = []
        for key in self.get_match_keys(base_path):
            keys.append(key)
        return keys


    def get_match_keys(self, base_path):
        bucket = re.findall("s3://([a-zA-Z\-]*)/", base_path)[0]
        prefix = re.findall("s3://[a-zA-Z\-]*/(.*)", base_path)[0]
        kwargs = {'Bucket': bucket}
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix
        while True:
            resp = self._s3.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and not key.endswith('_SUCCESS'):
                    full_path = "s3://" + bucket + "/" + key
                    yield full_path
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def parse_and_group_filenames(self, s3_files):
        """
        Return a list with grouped and parsed S3 files
        Example [{"from_ts": DT, "to_ts": DT, "s3_files": ["1", "2"]}, ...]
        """
        all_tasks = []
        s3_files = sorted(s3_files)
        for fullpath in s3_files:
            filename = fullpath.split("/")
            if len(filename) == 8 and filename[-1] != '':
                last_task = Task([fullpath])
                all_tasks.append(last_task)
        return all_tasks

    def upload_file(self, bucket_name, key, data):
        self._s3.put_object(Bucket=bucket_name, Key=key, Body=data)

    def move_files(self, orig_bucket, orig_key, new_bucket, new_key):
        copy_source = {
            'Bucket': orig_bucket,
            'Key': orig_key
        }
        self._s3.copy_object(CopySource=copy_source, Bucket=new_bucket, Key=new_key)
        self._s3.delete_object(Bucket=orig_bucket, Key=orig_key)

    def get_single_object(self, bucket, key):
        if key is not None:
            obj = self._s3.get_object(Bucket=bucket, Key=key)
            return obj
        else:
            logging.warn("Key is not set.")
            return None

    def get_object_list_from_s3(self, s3_bucket_name, prefix, object_filter=None):
        """
        Return a list of keys that is in the s3_bucket_name/prefix and meet the object filter, but object filter is optional
        """
        paginator = self._s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=s3_bucket_name, Prefix=prefix)
        object_list = []
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    key_string = key["Key"]
                    if object_filter is not None:
                        if object_filter in key_string:
                            object_list.append(key_string)
                    else:
                        object_list.append(key_string)

        return object_list

    def delete_file(self, bucket, key):
        self._s3.delete_object(Bucket=bucket, Key=key)

class Task:
    def __init__(self, s3_files):
        self.s3_files = s3_files
        self.id = ""
        self.status = ""
