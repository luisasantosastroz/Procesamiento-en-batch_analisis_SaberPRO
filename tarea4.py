from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('SaberPro').getOrCreate()

file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

df = spark.read.format('csv') \
    .option('header','true') \
    .option('inferSchema','true') \
    .load(file_path)

print("=== ESQUEMA ===")
df.printSchema()

print("=== LIMPIEZA ===")
df = df.dropna(subset=[
    "FAMI_TIENEINTERNET",
    "MOD_INGLES_PUNT",
    "MOD_LECTURA_CRITICA_PUNT"
])

df = df.dropDuplicates()

# Convertir a números
df = df.withColumn("MOD_INGLES_PUNT", F.col("MOD_INGLES_PUNT").cast(IntegerType()))
df = df.withColumn("MOD_LECTURA_CRITICA_PUNT", F.col("MOD_LECTURA_CRITICA_PUNT").cast(IntegerType()))

# Crear promedio
df = df.withColumn(
    "PROMEDIO",
    (F.col("MOD_INGLES_PUNT") + F.col("MOD_LECTURA_CRITICA_PUNT")) / 2
)

print("=== ANÁLISIS ===")

print("Promedio según internet")
df.groupBy("FAMI_TIENEINTERNET").avg("PROMEDIO").show()

print("Promedio por departamento")
df.groupBy("ESTU_DEPTO_RESIDE") \
  .avg("PROMEDIO") \
  .orderBy(F.col("avg(PROMEDIO)").desc()) \
  .show()

print("Promedio según educación de la madre")
df.groupBy("FAMI_EDUCACIONMADRE").avg("PROMEDIO").show()