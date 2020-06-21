from airflow.plugins_manager import AirflowPlugin

import logging as log
#for custom hooks
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook


class MySQLToPostgresHook(BaseHook):
    def __init__(self):
        print("##custom hook started##")

    def copy_table(self, mysql_conn_id, postgres_conn_id):

        print("### fetching records from MySQL table ###")
        mysqlserver = MySqlHook(mysql_conn_id)
        sql_query = "SELECT * from clean_store_transactions "
        data = mysqlserver.get_records(sql_query)

        print("### inserting records into Postgres table ###")
        postgresserver = PostgresHook(postgres_conn_id)
        postgres_query = "INSERT INTO clean_store_transactions VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
        postgresserver.insert_rows(table='clean_store_transactions', rows=data)

        return True



class DemoPlugin(AirflowPlugin):
    name = "demo_plugin"
    operators = []
    sensors = []
    hooks = [MySQLToPostgresHook]
