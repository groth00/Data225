import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


## @type: DataSource
## @args: [database = "yelp", table_name = "yelp_restaurants_csv", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "yelp", table_name = "yelp_restaurants_csv", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("business_id", "string", "business_id", "string"), ("name", "string", "name", "string"), ("address", "string", "address", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("postal_code", "string", "postal_code", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("stars", "double", "stars", "double"), ("review_count", "long", "review_count", "int"), ("is_open", "long", "is_open", "int")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("business_id", "string", "business_id", "string"), ("name", "string", "name", "string"), ("address", "string", "address", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("postal_code", "string", "postal_code", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("stars", "double", "stars", "double"), ("review_count", "long", "review_count", "int"), ("is_open", "long", "is_open", "int")], transformation_ctx = "Transform0")
## @type: DropFields
## @args: [paths = ['latitude', 'longitude', 'stars']]
## @return: DropFields0
## @inputs: [frame = Transform0]
DropFields0 = DropFields.apply(frame = Transform0, paths = ['latitude', 'longitude', 'stars'], transformation_ctx = 'DropFields0')
## @type: DataSink
## @args: [catalog_connection = "yelp-redshift", connection_options = {"dbtable": "business", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = DropFields0]
DataSink0 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = DropFields0, catalog_connection = "yelp-redshift", connection_options = {"dbtable": "business", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "DataSink0")


## @type: DataSource
## @args: [database = "yelp", table_name = "yelp_users_csv", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "yelp", table_name = "yelp_users_csv", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("user_id", "string", "user_id", "string"), ("name", "string", "name", "string"), ("review_count", "long", "review_count", "int"), ("yelping_since", "string", "yelping_since", "timestamp"), ("useful", "long", "useful", "int"), ("funny", "long", "funny", "int"), ("cool", "long", "cool", "int"), ("fans", "long", "fans", "int"), ("average_stars", "double", "average_stars", "decimal"), ("compliment_hot", "long", "compliment_hot", "int"), ("compliment_more", "long", "compliment_more", "int"), ("compliment_profile", "long", "compliment_profile", "int"), ("compliment_cute", "long", "compliment_cute", "int"), ("compliment_list", "long", "compliment_list", "int"), ("compliment_note", "long", "compliment_note", "int"), ("compliment_plain", "long", "compliment_plain", "int"), ("compliment_cool", "long", "compliment_cool", "int"), ("compliment_funny", "long", "compliment_funny", "int"), ("compliment_writer", "long", "compliment_writer", "int"), ("compliment_photos", "long", "compliment_photos", "int")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource1]
Transform1 = ApplyMapping.apply(frame = DataSource1, mappings = [("user_id", "string", "user_id", "string"), ("name", "string", "name", "string"), ("review_count", "long", "review_count", "int"), ("yelping_since", "string", "yelping_since", "timestamp"), ("useful", "long", "useful", "int"), ("funny", "long", "funny", "int"), ("cool", "long", "cool", "int"), ("fans", "long", "fans", "int"), ("average_stars", "double", "average_stars", "decimal"), ("compliment_hot", "long", "compliment_hot", "int"), ("compliment_more", "long", "compliment_more", "int"), ("compliment_profile", "long", "compliment_profile", "int"), ("compliment_cute", "long", "compliment_cute", "int"), ("compliment_list", "long", "compliment_list", "int"), ("compliment_note", "long", "compliment_note", "int"), ("compliment_plain", "long", "compliment_plain", "int"), ("compliment_cool", "long", "compliment_cool", "int"), ("compliment_funny", "long", "compliment_funny", "int"), ("compliment_writer", "long", "compliment_writer", "int"), ("compliment_photos", "long", "compliment_photos", "int")], transformation_ctx = "Transform1")
## @type: DataSink
## @args: [catalog_connection = "yelp-redshift", connection_options = {"dbtable": "user_info", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "DataSink1"]
## @return: DataSink1
## @inputs: [frame = Transform1]
DataSink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = Transform1, catalog_connection = "yelp-redshift", connection_options = {"dbtable": "user_info", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "DataSink1")


## @type: DataSource
## @args: [database = "yelp", table_name = "yelp_reviews_notext_csv", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "yelp", table_name = "yelp_reviews_notext_csv", transformation_ctx = "DataSource2")
## @type: ApplyMapping
## @args: [mappings = [("review_id", "string", "review_id", "string"), ("user_id", "string", "user_id", "string"), ("business_id", "string", "business_id", "string"), ("stars", "long", "stars", "decimal"), ("useful", "long", "useful", "int"), ("funny", "long", "funny", "int"), ("cool", "long", "cool", "int"), ("date", "string", "date", "timestamp")], transformation_ctx = "Transform1"]
## @return: Transform2
## @inputs: [frame = DataSource2]
Transform2 = ApplyMapping.apply(frame = DataSource2, mappings = [("review_id", "string", "review_id", "string"), ("user_id", "string", "user_id", "string"), ("business_id", "string", "business_id", "string"), ("stars", "long", "stars", "decimal"), ("useful", "long", "useful", "int"), ("funny", "long", "funny", "int"), ("cool", "long", "cool", "int"), ("date", "string", "date", "timestamp")], transformation_ctx = "Transform2")
## @type: DataSink
## @args: [catalog_connection = "yelp-redshift", connection_options = {"dbtable": "review", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "DataSink2"]
## @return: DataSink2
## @inputs: [frame = Transform2]
DataSink2 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = Transform2, catalog_connection = "yelp-redshift", connection_options = {"dbtable": "review", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "DataSink2")


## @type: DataSource
## @args: [database = "yelp", table_name = "annotated_reviews_csv", transformation_ctx = "DataSource4"]
## @return: DataSource3
## @inputs: []
DataSource3 = glueContext.create_dynamic_frame.from_catalog(database = "yelp", table_name = "annotated_reviews_csv", transformation_ctx = "DataSource3")
## @type: DataSink
## @args: [catalog_connection = "yelp-redshift", connection_options = {"dbtable": "categories", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "DataSink4"]
## @return: DataSink3
## @inputs: [frame = DataSource3]
DataSink3 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = DataSource3, catalog_connection = "yelp-redshift", connection_options = {"dbtable": "annotations", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "DataSink3")


## @type: DataSource
## @args: [database = "yelp", table_name = "yelp_restaurants_categories", transformation_ctx = "DataSource4"]
## @return: DataSource4
## @inputs: []
DataSource4 = glueContext.create_dynamic_frame.from_catalog(database = "yelp", table_name = "yelp_restaurants_categories_csv", transformation_ctx = "DataSource4")
## @type: DataSink
## @args: [catalog_connection = "yelp-redshift", connection_options = {"dbtable": "categories", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "DataSink4"]
## @return: DataSink4
## @inputs: [frame = DataSource4]
DataSink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = DataSource4, catalog_connection = "yelp-redshift", connection_options = {"dbtable": "categories", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "DataSink4")

job.commit()