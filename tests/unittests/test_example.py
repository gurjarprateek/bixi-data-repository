import unittest
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator

def ret_hello():
    hello = 'Hello'
    return hello

class TestExample(unittest.TestCase):

    def test_execute(self):
        dag = DAG(dag_id='example', start_date=datetime.now())
        task = PythonOperator(python_callable=ret_hello, task_id='test_python', dag=dag)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(ti.get_template_context())
        self.assertEqual(result, 'Hello')