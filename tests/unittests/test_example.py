import unittest
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators import BashOperator


class TestExample(unittest.TestCase):

    def test_execute(self):
        dag = DAG(dag_id='example', start_date=datetime.now())
        task = BashOperator(dag=dag, task_id='print_date')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        result = task.execute(ti.get_template_context())
        self.assertEqual(result, '2019-08-31')


suite = unittest.TestLoader().loadTestsFromTestCase(TestExample)
unittest.TextTestRunner(verbosity=2).run(suite)