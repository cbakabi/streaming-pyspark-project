# new dataframe with those that have done an order
# extract user id 
# find their first event 
# extract marketing channel from first event.
# create table called purchase_marketing_attributions in ecommerce-project-analytical-database
# record data in table including the user ID, the order ID, and the marketing channel name.

from pyspark.sql import SparkSession
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql.functions import col, from_csv, from_json, when
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import udf, lit
from pyspark.sql.functions import to_json, struct


def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/100000

kafka_options = {
    "kafka.bootstrap.servers": "b-2-public.kafkaecommerceprojectc.2qbbj8.c3.kafka.eu-west-2.amazonaws.com:9198",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="default";""",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "earliest", # Start from the latest event when we consume from kafka
    "subscribe": "processed-events"        # Our topic name
}

# Define the database connection properties
db_properties = {
    'user': '<db username>',  # Replace with your RDS username
    'password': '<db password>',  # Replace with your RDS password
    'driver': 'org.postgresql.Driver'  # PostgreSQL JDBC driver
}

# JDBC URL for the AWS RDS PostgreSQL instance
db_url = "jdbc:postgresql://ecommerce-project-analytical-database.cfmnnswnfhpn.eu-west-2.rds.amazonaws.com:5432/ecommerce-project-analytical-database"

# Table name where the data will be inserted
table_name = "purchase_marketing_attributions"

spark = SparkSession.builder.appName("IntroToPySpark") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.2.18') \
    .config("spark.jars", "kafka_2.13-3.9.0/libs/aws-msk-iam-auth-1.1.1-all.jar") \
    .getOrCreate()

def parse_json_dataframe(json_df):
    def getKey(dictionary, key):
        return dictionary.get(key)

    udfGetKey = udf(getKey)


    # Deserialize the JSON value from Kafka as a String
    # and convert it to a dict
    return json_df\
        .withColumn("value", from_json(
            col('value').cast("string"),
            MapType(StringType(), StringType())
        )) \
        .withColumn('user_id', udfGetKey(col('value'), lit('user_id'))) \
        .withColumn('event_name', udfGetKey(col('value'), lit('event_name'))) \
        .withColumn('page', udfGetKey(col('value'), lit('page'))) \
        .withColumn('item_url', udfGetKey(col('value'), lit('item_url'))) \
        .withColumn('order_email', udfGetKey(col('value'), lit('order_email'))) \
        .withColumn('channel', udfGetKey(col('value'), lit('channel'))) \
        .select('user_id', 'event_name', 'page', 'item_url', 'order_email', 'channel')

df = spark.readStream.format("kafka").options(**kafka_options).load()

parsed_df = parse_json_dataframe(df)

visit_df = parsed_df.filter((col("event_name") == "visit") & (col("page") == "/purchase"))
order_confirmed_df = parsed_df.filter(col("event_name") == "order_confirmed")

final_df = visit_df.join(order_confirmed_df, 'user_id', 'inner').select(visit_df['*'])
final_df = final_df.select('user_id', 'event_name', 'page', 'order_email', 'channel')

final_df = final_df.withColumn("channel", when(col("channel").isNull(), "organic").otherwise(col("channel")))

def write_to_rds(final_df):
# Writing the DataFrame to the database table
 final_df.write \
   .jdbc(url=db_url, table=table_name, mode="append", properties=db_properties)

query = final_df \
        .writeStream\
        .foreachBatch(write_to_rds) \
        .outputMode('append') \
        .format('console') \
        .option('truncate', 'false') \
        .start()
