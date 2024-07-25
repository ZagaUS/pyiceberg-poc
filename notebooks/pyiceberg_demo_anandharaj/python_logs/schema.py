from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    LongType,
    IntegerType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
    ListType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# Define the schema for the spans
schema = Schema(
    NestedField(field_id=1, name="servicename", field_type=StringType(), required=False),
    NestedField(field_id=2, name="language", field_type=StringType(), required=False),
    NestedField(field_id=3, name="logsdata", field_type=StringType(), required=False),
    NestedField(field_id=4, name="createdTime", field_type=TimestampType(), required=False),

)

# partition_spec = PartitionSpec(
#     PartitionField(
#         source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
#     )
# )

sort_order = SortOrder(SortField(source_id=3, transform=IdentityTransform()))


def create_table_logs(namespace,table_name,catalog):
    try:
        table = catalog.create_table(
            identifier=(namespace, table_name),
            schema=schema,
            location="s3a://apmlogs/logs", #mention s3 bucketname
            # partition_spec=partition_spec,
            sort_order=sort_order,
        )
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"error in create table logs config {e}")
    return table