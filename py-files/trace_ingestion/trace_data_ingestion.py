from pyiceberg.catalog import load_catalog
from kafka import KafkaConsumer
import json
catalog = load_catalog(
        "docs",
    **{
            "uri": "thrift://hive-ms-trino.apps.zagaopenshift.zagaopensource.com:9888",
            "s3.endpoint": "http://minio-lb.apps.zagaopenshift.zagaopensource.com:9009",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.access-key-id": "minioAdmin",
            "s3.secret-access-key": "minio1234",
    }
)
logstable=catalog.load_table("docs_logs.logs") #mention tablename and namesapce