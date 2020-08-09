#{"user_fname":"sunil", "user_lname":"miriyala", "user_id":"sunil.miriyala", "updated_at":"2020-03-22 14:15:16.000", "modified_fields":[{"address":"new value"}]}

#For: ORC
create external table user_profile (user_fname string, user_lname string, user_id string, updated_at string, modified_fields array<string>) PARTITIONED BY (datepart string) ROW FORMAT DELIMITED STORED AS ORC LOCATION '/tmp/Spark-Kafka-Stream/Output-ORC/';

#For: AVRO

