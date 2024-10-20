import logging
from airflow.models import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.decorators import apply_defaults


logger = logging.getLogger(__name__)


class BigQueryQueryOperator(BaseOperator):

    template_fields = ("sql",)
    template_ext = (".sql",)

    @apply_defaults
    def __init__(self, sql=None, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self, context):
        try:

            hook = BigQueryHook(use_legacy_sql=False)
            records = hook.get_records(sql=self.sql)

            record_count = len(records)
            logger.info(
                f"Query executed successfully. Number of records retrieved: {record_count}"
            )

            for row in records:
                logging.info(f"{row}")

        except Exception as err:
            logging.error(f"Query execution failed: {err}")
            raise
