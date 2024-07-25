from pyiceberg.catalog import load_catalog

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    TimestampType,
    LongType,
    IntegerType,
    StringType,
    NestedField,
    StructType,
    ListType
)
from pyiceberg.schema import Schema

from pyiceberg.io.pyarrow import PyArrowFileIO
import pyarrow as pa
import json

from kafka_connection import consuming_message


data=consuming_message()
namespace=input("enter the namespace:")
tablename=input("enter your table name:")

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

catalog.create_namespace(f"{namespace}")

catalog.list_namespaces()


def table_append():
    schema = Schema(
        NestedField(
            field_id=1,
            name="tracedata",  
            field_type=StringType(),
            required=False
        )
    )


    table = catalog.create_table(
            identifier=f"{namespace}.{tablename}", 
            schema=schema ,
            location="s3a://messageingest/trace"
           
        )
    serialized_data = [json.dumps(msg) for msg in data]



    # Create a PyArrow Table with a single column containing the serialized data
    df = pa.Table.from_pylist([{"tracedata": data} for data in serialized_data])

    table.append(df)
    print("sucessfully appended")

    return table.scan().to_arrow()

table_append()

