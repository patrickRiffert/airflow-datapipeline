from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook
from datetime import datetime
import json
from pathlib import Path
from os.path import join


class TwitterOperator(BaseOperator):

    # template_fields = ['query', 'file_path']

    @apply_defaults
    def __init__(self,
        query,
        file_path,
        conn_id = None,
        *args, **kwargs
        ):
        super().__init__(*args, **kwargs) #*args, **kwargs
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id


    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)


    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id
        )
        self.create_parent_folder()
        data = hook.run()
        with open(self.file_path, 'w') as output_file:
            json.dump(data, output_file, ensure_ascii=False)
        # with hook.run() as data:
        #     print(json.dumps(data, indent=4, sort_keys=True))



if __name__ == '__main__':
    with DAG(dag_id='TwitterTest', start_date=datetime.now()) as dag:
        to = TwitterOperator(query='Boticário',
            file_path=join(
                '/home/priffert/datapipe/datalake/bronze',
                'boticario_twetts',
                f'dt_extraction={datetime.now().strftime("%Y-%m-%d")}',
                f'tweets_{datetime.now().strftime("%Y-%m-%d")}.json'
            ),
            task_id='test_run'
        )
        # ti = TaskInstance(task=to, execution_date=datetime.now())
        # to.execute(ti.get_template_context())
        to.execute()