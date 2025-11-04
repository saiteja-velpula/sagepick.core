import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    bucket: str
    key: str
    version_id: Optional[str]


class S3DatasetWriter:
    """Small wrapper around boto3 client to upload dataset snapshots.

    Responsibilities:
    - Build a boto3 client from provided config
    - Upload a file to a dated key (prefix/YYYY-MM-DD/file_name)
    - Optionally copy to a stable latest key (prefix/file_name)
    - Return version ids for traceability
    """

    def __init__(self, *, bucket: str, prefix: str, file_name: str, endpoint_url: Optional[str] = None, access_key: Optional[str] = None, secret_key: Optional[str] = None, region_name: Optional[str] = None, use_ssl: bool = True):
        self.bucket = bucket
        self.prefix = (prefix or "").strip("/")
        self.file_name = file_name
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name
        self.use_ssl = use_ssl
        self._client = None

    def _client_kwargs(self):
        kw = {
            "service_name": "s3",
            "use_ssl": self.use_ssl,
        }
        if self.endpoint_url:
            kw["endpoint_url"] = self.endpoint_url
        if self.region_name:
            kw["region_name"] = self.region_name
        if self.access_key:
            kw["aws_access_key_id"] = self.access_key
        if self.secret_key:
            kw["aws_secret_access_key"] = self.secret_key
        return kw

    def client(self):
        if self._client is None:
            self._client = boto3.client(**self._client_kwargs())
        return self._client

    def dated_key(self, timestamp) -> str:
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        parts.append(timestamp.strftime("%Y-%m-%d"))
        parts.append(self.file_name)
        return "/".join(parts)

    def latest_key(self) -> str:
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        parts.append(self.file_name)
        return "/".join(parts)

    async def upload_file(self, source_path: str, key: str) -> UploadResult:
        """Upload file and return UploadResult with VersionId if available."""
        def _put():
            with open(source_path, "rb") as fh:
                return self.client().put_object(Bucket=self.bucket, Key=key, Body=fh, ContentType="text/csv")

        try:
            resp = await asyncio.to_thread(_put)
            version_id = resp.get("VersionId")
            logger.debug("Uploaded %s/%s (version=%s)", self.bucket, key, version_id)
            return UploadResult(bucket=self.bucket, key=key, version_id=version_id)
        except (BotoCoreError, ClientError) as exc:
            logger.error("Upload failed for %s/%s: %s", self.bucket, key, exc)
            raise

    async def copy_to_latest(self, source_key: str) -> UploadResult:
        """Copy an existing object to the stable latest key. Returns UploadResult of copy target."""
        dest = self.latest_key()

        def _copy():
            return self.client().copy_object(Bucket=self.bucket, Key=dest, CopySource={"Bucket": self.bucket, "Key": source_key}, MetadataDirective="COPY")

        try:
            resp = await asyncio.to_thread(_copy)
            version_id = resp.get("VersionId")
            logger.debug("Copied %s -> %s/%s (version=%s)", source_key, self.bucket, dest, version_id)
            return UploadResult(bucket=self.bucket, key=dest, version_id=version_id)
        except (BotoCoreError, ClientError) as exc:
            logger.error("Copy to latest failed for %s -> %s: %s", source_key, dest, exc)
            raise
