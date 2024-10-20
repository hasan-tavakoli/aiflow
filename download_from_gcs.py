from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import logging

logger = logging.getLogger(__name__)


class GCSDownloadOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, bucket_name: str, file_name: str, destination_path: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.destination_path = destination_path

    def execute(self, context):
        gcs_hook = GCSHook()
        try:
            gcs_hook.download(
                bucket_name=self.bucket_name,
                object_name=self.file_name,
                filename=self.destination_path,
            )
            logger.info(
                f"Successfully downloaded data from GCS bucket '{self.bucket_name}' to '{self.destination_path}'"
            )
        except Exception as e:
            logger.error(f"Failed to download data from GCS: {str(e)}")
            raise
