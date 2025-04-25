from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import signal
import sys

# Конфігурація
STUDENT_ID = "maksym_kuz4"
KAFKA_INPUT_TOPIC = f"athlete_events_{STUDENT_ID}"
MYSQL_OUTPUT_TABLE = f"athlete_stats_{STUDENT_ID}"
BATCH_SIZE = 500  # Розмір батчу для Kafka

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('consumer.log')
    ]
)
logger = logging.getLogger(__name__)

# Схема даних
event_schema = StructType([
    StructField("edition", StringType()),
    StructField("edition_id", IntegerType()),
    StructField("country_noc", StringType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("result_id", IntegerType()),
    StructField("athlete", StringType()),
    StructField("athlete_id", IntegerType()),
    StructField("pos", StringType()),
    StructField("medal", StringType()),
    StructField("isTeamSport", StringType())
])

# Глобальні змінні для graceful shutdown
query = None
spark = None

def graceful_shutdown(signum, frame):
    logger.info("Отримано сигнал завершення, зупиняємо потокову обробку...")
    if query:
        query.stop()
    if spark:
        spark.stop()
    sys.exit(0)

def create_spark_session():
    """Ініціалізація Spark з оптимізованими налаштуваннями"""
    return SparkSession.builder \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .appName("OlympicStatsConsumer") \
        .getOrCreate()

def read_bio_data(spark):
    """Завантаження біографічних даних з MySQL"""
    logger.info("Завантаження даних атлетів...")
    return spark.read.format("jdbc").options(
        url="jdbc:mysql://217.61.57.46:3306/olympic_dataset",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="athlete_bio",
        user="neo_data_admin",
        password="Proyahaxuqithab9oplp",
        fetchSize="10000"
    ).load().filter(
        (col("height").isNotNull()) & 
        (col("weight").isNotNull()) &
        (col("height") != 0) & 
        (col("weight") != 0)
    )

def create_kafka_stream(spark):
    """Створення потокового з'єднання з Kafka"""
    logger.info(f"Підключення до Kafka топіку {KAFKA_INPUT_TOPIC}")
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 
               'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
        .option("subscribe", KAFKA_INPUT_TOPIC)  \
        .option("maxOffsetsPerTrigger", "500") \
        .option("failOnDataLoss", "false") \
        .load()
        # .option("startingOffsets", "earliest") \

def process_batch(batch_df, batch_id):
    """Обробка одного батчу даних"""
    try:
        # Виводимо інформацію про батч
        logger.info(f"\n=== Батч {batch_id} ===")
        logger.info(f"Кількість записів: {batch_df.count()}")
        
        # Виводимо перші 3 рядки для перевірки
        logger.info("Перші 3 рядки даних:")
        batch_df.show(3, truncate=False)
        
        # Запис у MySQL
        logger.info("Початок запису в MySQL...")
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
            .option("dbtable", MYSQL_OUTPUT_TABLE) \
            .option("user", "neo_data_admin") \
            .option("password", "Proyahaxuqithab9oplp") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", "500") \
            .mode("append") \
            .save()
        
        logger.info(f"Успішно записано батч {batch_id}\n")
    except Exception as e:
        logger.error(f"Помилка обробки батчу {batch_id}: {str(e)}", exc_info=True)
        raise

def main():
    global query, spark
    
    # Реєстрація обробників сигналів
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    try:
        spark = create_spark_session()
        logger.info("Ініціалізація Spark завершена")
        
        # Завантаження довідкових даних
        bio_df = read_bio_data(spark).cache()
        logger.info(f"Завантажено {bio_df.count()} записів про атлетів")
        
        # Потокова обробка
        kafka_stream = create_kafka_stream(spark)
        
        # Парсинг JSON
        parsed_df = kafka_stream.select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("data.*")
        
        # Об'єднання з біографічними даними
        joined_df = parsed_df.join(
            broadcast(bio_df.select("athlete_id", "sex", "height", "weight")),
            "athlete_id"
        )
        
        # Агрегація даних
        stats_df = joined_df.groupBy(
            "sport", "medal", "sex", "country_noc"
        ).agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            count("*").alias("athlete_count"),
            current_timestamp().alias("calculation_time")
        )
                    # .outputMode("complete") \

        # Запуск потокової обробки
        query = stats_df.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/spark-checkpoint") \
            .foreachBatch(process_batch) \
            .start()
        
        logger.info("Потокова обробка запущена. Очікування даних...")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Критична помилка: {str(e)}", exc_info=True)
    finally:
        if spark:
            spark.stop()
        logger.info("Обробка завершена")

if __name__ == "__main__":
    main()