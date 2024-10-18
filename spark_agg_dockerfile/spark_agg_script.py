from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, count, when, sum
import sys
from datetime import datetime, timedelta
import os
import logging
import pandas as pd

run_date_str = sys.argv[1]
run_date = datetime.strptime(run_date_str, "%Y-%m-%d")

input_dir = "/opt/spark/input/" # CRUD данные за каждый день
daily_agg_dir = "/opt/spark/daily_agg/" # предагрегированные файлы за каждый день
output_file_dir = "/opt/spark/output/" # результаты недельной агрегации

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s: %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S' 
)
logger = logging.getLogger(__name__)  

# Определяем даты за 7 дней до запуска DAG 
start_date = run_date - timedelta(days=7)  # Это начало диапазона
end_date = run_date - timedelta(days=1)  # Это конец диапазона

# Создаем список дат в формате строки для работы с файлами
agg_date_range = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]

agg_dates_dict = {key: None for key in agg_date_range} # словарь для сбора информации по файлам

logger.info(f"Вычисление агрегата за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}")  # для отладки

# Получаем список файлов в папке с входными данными
all_input_files_list = os.listdir(input_dir)
# Отфильтровываем только CSV, содержащие в названии дату из нужного диапазона
filtered_files_list = [os.path.join(input_dir, f) for f in all_input_files_list if f.endswith(".csv") and f[:10] in agg_date_range]

# Если отфильтрованные файлы есть - логируем список, если нет — логируем ошибку и выходим
if filtered_files_list:
    logger.info("Файлы для агрегации:")
    for file in filtered_files_list:
        logger.info(file)  # Вывод списка файлов для агрегации
else:
    logger.error("Не найдено файлов для агрегации за данный период!")  # Если не нашли
    sys.exit(1)  # Выходим из программы с ошибкой


spark = SparkSession.builder.appName("User Actions Aggregation").getOrCreate()

schema = StructType([
    StructField("email", StringType(), True),  # email
    StructField("action", StringType(), True),  # действие пользователя
    StructField("dt", StringType(), True)  # дата
])

weekly_agg_list = []  # Список предагрегированных файлов для итоговой агрегации

for agg_date in agg_date_range:

    input_file = os.path.join(input_dir, f"{agg_date}.csv")
    daily_agg_file = os.path.join(daily_agg_dir, f"{agg_date}_agg.csv")

    if os.path.exists(daily_agg_file): # Проверка наличия предагрегированного файла
        weekly_agg_list.append(daily_agg_file) # Добавляем файл в список для итоговой (недельной) агрегации
        agg_dates_dict[agg_date] = 'pre-aggregated file was used'
    
    elif os.path.exists(input_file): # Если среди преагрегированных нет - то проверка наличия файла в input_dir
        
        df = spark.read.csv(input_file, schema=schema, header=False)
        
        agg_df = df.groupBy("email").agg(
            count(when(col("action") == "CREATE", True)).alias("create_count"),
            count(when(col("action") == "READ", True)).alias("read_count"),
            count(when(col("action") == "UPDATE", True)).alias("update_count"),
            count(when(col("action") == "DELETE", True)).alias("delete_count")
        )

        agg_pd_df = agg_df.toPandas()
        agg_pd_df.to_csv (daily_agg_file, mode='w', header=True, index = False)
        weekly_agg_list.append(daily_agg_file) # Добавляем файл в список для итоговой (недельной) агрегации
        agg_dates_dict[agg_date] = 'aggregation was performed'
    
    else:
        agg_dates_dict[agg_date] = 'aggregation was not performed because there was no file for this date'

agg_schema = StructType([
    StructField("email", StringType(), True),  # email
    StructField("create_count", LongType(), True),  # Кол-во create
    StructField("read_count", LongType(), True),  # Кол-во read
    StructField("update_count", LongType(), True),  # Кол-во update
    StructField("delete_count", LongType(), True)  # Кол-во delete
])

df = spark.read.csv(weekly_agg_list, schema=agg_schema, header=True)

weekly_agg_df = df.groupBy("email").agg(
    sum("create_count").alias("create_sum"),
    sum("read_count").alias("read_sum"),
    sum("update_count").alias("update_sum"),
    sum("delete_count").alias("delete_sum")
)

weekly_agg_file = os.path.join(output_file_dir, f"{run_date.strftime('%Y-%m-%d')}_agg.csv")
weekly_agg_pd_df = weekly_agg_df.toPandas()
weekly_agg_pd_df.to_csv (weekly_agg_file, mode='w', header=True, index = False)

logger.info("Произведена недельная агрегация по следующим датам:")
for key in sorted(agg_dates_dict):
    logger.info(f"{key}: {agg_dates_dict[key]}")

logger.info(f"Итоговый файл сохранен в {weekly_agg_file}")

spark.stop()