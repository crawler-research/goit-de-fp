# producer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

# Унікальний ідентифікатор
STUDENT_ID = "maksym_kuz4"
# Конфігурація Kafka з УНІКАЛЬНОЮ назвою топіку
KAFKA_TOPIC = f"athlete_events_{STUDENT_ID}"  #

# Конфігурація
kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}


mysql_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "table": "athlete_event_results"
}

# Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .appName("JDBCToKafka") \
    .getOrCreate()


# Читання даних з MySQL
df = spark.read.format('jdbc').options(
    url=mysql_config["url"],
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=mysql_config["table"],
    user=mysql_config["user"],
    password=mysql_config["password"]
).load()

print("Перевірка даних з athlete_event_results:")
df.show(5)

# Конвертація в JSON та запис у Kafka
kafka_df = df.select(to_json(struct("*")).alias("value"))

print("Приклад JSON даних для Kafka:")
kafka_df.show(5, truncate=False)

kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("topic", KAFKA_TOPIC) \
    .save()

print(f"Дані успішно записані у Kafka топік {KAFKA_TOPIC}!")
spark.stop()