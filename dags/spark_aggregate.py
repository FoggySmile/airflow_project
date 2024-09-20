from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys

# Инициализируем SparkSession
def main(target_date_str):
    spark = SparkSession.builder.appName("WeeklyAggregate").getOrCreate()

    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    start_date = target_date - timedelta(days=7)
    end_date = target_date - timedelta(days=1)

    input_dir = '/opt/airflow/input'
    output_dir = '/opt/airflow/output'

    # Список файлов для чтения
    date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    file_paths = [f"{input_dir}/{date}.csv" for date in date_list]

    # Читаем данные
    df = spark.read.csv(file_paths, schema="email STRING, action STRING, dt STRING", header=False)

    # Агрегируем данные
    result = df.groupBy("email", "action").count().groupBy("email").pivot("action").sum("count").fillna(0)

    # Переименовываем колонки
    result = result.withColumnRenamed("CREATE", "create_count") \
                   .withColumnRenamed("READ", "read_count") \
                   .withColumnRenamed("UPDATE", "update_count") \
                   .withColumnRenamed("DELETE", "delete_count")

    # Сохраняем результат
    output_file = f"{output_dir}/{target_date_str}.csv"
    result.coalesce(1).write.csv(output_file, header=True, mode='overwrite')

    spark.stop()

if __name__ == "__main__":
    target_date_str = sys.argv[1]
    main(target_date_str)
