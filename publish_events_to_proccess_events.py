from pyspark.sql import SparkSession
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql.functions import col, from_csv, from_json
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
    "subscribe": "events"        # Our topic name
}

spark = SparkSession.builder.appName("IntroToPySpark") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
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

updated_email_df = parsed_df.withColumn("order_email", lit("order@order.com"))

final_df = updated_email_df.withColumn("value", to_json(struct(*updated_email_df.columns)))

query = final_df \
        .writeStream\
        .outputMode('append') \
        .option("kafka.bootstrap.servers", "b-2-public.kafkaecommerceprojectc.2qbbj8.c3.kafka.eu-west-2.amazonaws.com:9198") \
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="default";""") \
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
        .option("topic", "processed-events") \
        .option("checkpointLocation", "spark_checkpoint_log") \
        .format('kafka') \
        .start()