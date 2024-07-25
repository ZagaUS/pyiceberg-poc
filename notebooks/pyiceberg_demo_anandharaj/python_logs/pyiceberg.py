from pyiceberg.catalog import load_catalog
import os
namespace=os.getenv("NAME_SPACE", "docs_logs")
table_name=os.getenv("TABLE_NAME", "logs")
def load_catalog_config ():
        try:
                catalog =  load_catalog(
                                "docs",
                        **{
                                "uri": "thrift://hive-ms-trino.apps.zagaopenshift.zagaopensource.com:9888",
                                "s3.endpoint": "http://minio-lb.apps.zagaopenshift.zagaopensource.com:9009",
                                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                                "s3.access-key-id": "minioAdmin",
                                "s3.secret-access-key": "minio1234",
                        }
                )
                print("catlog load suceessfully")
                ns=catalog.list_namespaces()
                print(ns)
                if((namespace,) in ns):
                        print("Namespace alredy exists")
                else:
                        catalog.create_namespace(namespace)
                        print("Namespace create suceesss")
                return catalog
        except Exception as e:
                print(f"error in load catlog config {e}")


 