import subprocess
import logging
import os
import tempfile
import time

from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable


def create_query_op(dag, task_id, query, db):
    return PostgresOperator(
      task_id=task_id,
      dag=dag,
      autocommit=True,
      postgres_conn_id=db,
      pool=db + "_pool",
      sql=query)


def zabbix_report(key, value, hostname):
    mon = Variable.get("zabbix_monitor")
    skip = Variable.get("zabbix_items").split(',')
    if "disabled" in mon or key in skip:
        return
    host, port = mon.split(":")
    logging.info("Zabbix >> %s : %s = %s" % (hostname, key, str(value)))
    attempt = 0
    with open("/dev/null") as null:
        while attempt < 10:
            attempt += 1
            r = subprocess.call(["zabbix_sender",
                                 "--key", str(key),
                                 "--value", str(value),
                                 "--zabbix-server", str(host),
                                 "--port", str(port),
                                 "--host", str(hostname)],
                                stdout=null)
            if not r:
                return
            logging.info("Zabbix reporting failed (code: %r) for %s : %s = %s (attempt: %i)" % (r, hostname, key, str(value), attempt))
            time.sleep(attempt * 5)
    logging.info("Max failed attempts reached, failing job")
    raise StandardError("Zabbix reporting failed")


def zabbix_report_multi(key_value_dict):
    mon = Variable.get("zabbix_monitor")
    if "disabled" in mon:
        return
    host, port = mon.split(":")
    attempt = 0
    with tempfile.NamedTemporaryFile() as f:
        f.writelines(["%s %s %s" % (str(k[0]), str(k[1]), str(v)) for k, v in key_value_dict.items()])
        f.flush()
        for k, v in key_value_dict.items():
            logging.info("Zabbix >> %s : %s = %s" % (k[0], k[1], str(v)))
        with os.tmpfile() as output:
            while attempt < 3:
                attempt += 1
                r = subprocess.call(["zabbix_sender",
                                     "--zabbix-server", str(host),
                                     "--port", str(port),
                                     "--input-file", str(f.name)],
                                    stdout=output)
                if not r:
                    return
                logging.info("Zabbix reporting failed (attempt: %i)" % (attempt))
                time.sleep(5)
    logging.info("Max failed attempts reached, failing job")
    raise StandardError("Zabbix reporting failed")


def s3_name_generator(input, prod, staging):
    env = Variable.get("environment")
    suffix = ""
    if env == "production":
        suffix = prod
    else:
        if env == "staging":
            suffix = staging
    return input + suffix


def get_env():
    env = Variable.get("environment")
    if env is None:
        return "default"
    return env
