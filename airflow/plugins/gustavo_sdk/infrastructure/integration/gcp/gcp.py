import csv
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
import google
from airflow.hooks.base import BaseHook
from google.cloud import storage
from google.oauth2 import service_account
from gustavo_sdk.infrastructure.common.utils import get_logger
import json

logger = get_logger(__name__)


class GCP:
    def __init__(
            self,
            service: google.cloud,
            credentials_file_path: Optional[str] = None,
            client: Optional[object] = None,
            credentials_json: Optional[str] = None,
            ) -> None:

        self.credentials_file_path = credentials_file_path
        self.credentials_json = credentials_json
        self.client = client or self._get_client(service)

        """
        Existe client?

        Sim:
            utiliza o client recebido e NÃO executa _get_client(service).

        Não:
            executa _get_client(service) para criar um novo client.

        _get_client(service) é utilizado quando ainda não existe um client.
        Ele cria um client autenticado que será armazenado em self.client.
        """
    def _get_client(self, service: google.cloud) -> google.cloud.client:
        # metodo da classe GCP

        """Obtains a client for the specified Google Cloud service.

        Args:
            service (google.cloud): The Google Cloud service class (e.g., `storage`, `bigquery`).

        Returns:
            google.cloud.client: Configured client instance.
        """

        if self.credentials_file_path:    # fluxo de decisão
            # caminho para um arquivo
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_file_path
            return service.Client()
        elif self.credentials_json:
            # não tenho o caminho mas tenho Json, usa Json


            info = self.credentials_json

            # não tenho nenhum dos dois, uso as credencias normal
            credentials = service_account.Credentials.from_service_account_info(info=info)
            return service.Client(credentials=credentials)
        else:
            return service.Client()


    def upload_file(
        self,
        bucket_name: str,
        bucket_prefix: str,
        data: list[dict],
        file_name: str,
    ) -> None:

        with TemporaryDirectory() as root:
            file_path = Path(root) / file_name

            with open(file_path, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)

            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(f"{bucket_prefix}/{file_name}")
            blob.upload_from_filename(str(file_path))

        logger.info("File uploaded to GCP successfully")