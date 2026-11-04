from pyspark.sql import functions as F
from pyspark.sql import SparkSession

#=======================
# Configuración
#=======================

CATALOG = "aerolinea"
SOURCE_SCHEMA = "fuentes"
BRONZE_SCHEMA = "bronze"
VOLUME_NAME = "landing"

VUELOS_PATH = f"/Volumes/{CATALOG}/{SOURCE_SCHEMA}/{VOLUME_NAME}/vuelos_diarios.csv"
AERONAVES_PAHT = f"/Volumes/{CATALOG}/{SOURCE_SCHEMA}/{VOLUME_NAME}/aeronaves.csv"
AEROPUERTOS_PAHT = f"/Volumes/{CATALOG}/{SOURCE_SCHEMA}/{VOLUME_NAME}/aeropuertos.csv"
MANTENIMIENTOS_PATH = f"/Volumes/{CATALOG}/{SOURCE_SCHEMA}/{VOLUME_NAME}/mantenimientos_rds.csv"

spark = SparkSession.builder.getOrCreate()

#==========================
# 0. Asegurar schema Bronze
#==========================

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

#==========================
# 1. Leer CSV
#==========================

#Leemos vuelos
vuelos_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(VUELOS_PATH)
)

#Leemos Aeronaves
aeronaves_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(AERONAVES_PAHT)
)

#Leemos Aeropuertos
aeropuertos_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(AEROPUERTOS_PAHT)
)

#Leemos Mantenimientos
mantenimientos_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(MANTENIMIENTOS_PATH)
)

#==========================
# 2. Normalizar tipos + ingest_timestamp
#==========================

vuelos_bronze = (
    vuelos_raw
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("vuelo_id", F.col("vuelo_id").cast("string"))
    .withColumn("origen_id", F.col("origen_id").cast("string"))
    .withColumn("destino_id", F.col("destino_id").cast("string"))
    .withColumn("aeronave_id", F.col("aeronave_id").cast("string"))
    .withColumn("estado", F.col("estado").cast("string"))
    .withColumn("duracion_min", F.col("duracion_min").cast("int"))
    .withColumn("ingestion_time", F.current_timestamp())
)

aeronaves_bronze = (
    aeronaves_raw
    .withColumn("aeronave_id", F.col("aeronave_id").cast("string"))
    .withColumn("modelo", F.col("modelo").cast("string"))
    .withColumn("fabricante", F.col("fabricante").cast("string"))
    .withColumn("anio_fabricacion", F.col("anio_fabricacion").cast("int"))
    .withColumn("ingestion_time", F.current_timestamp())
)

aeropuertos_bronze = (
    aeropuertos_raw
    .withColumn("aeropuerto_id", F.col("aeropuerto_id").cast("string"))
     .withColumn("nombre", F.col("nombre").cast("string"))
     .withColumn("ciudad", F.col("ciudad").cast("string"))
     .withColumn("pais", F.col("pais").cast("string"))
     .withColumn("lat", F.col("lat").cast("double"))
     .withColumn("lon", F.col("lon").cast("double"))
     .withColumn("ingestion_time", F.current_timestamp())
)

mantenimientos_bronze = (
    mantenimientos_raw
    .withColumn("mantenimiento_id", F.col("mantenimiento_id").cast("string"))
    .withColumn("aeronave_id", F.col("aeronave_id").cast("string"))
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("tipo", F.col("tipo").cast("string"))
    .withColumn("costo_usd", F.col("costo_usd").cast("double"))
    .withColumn("duracion_hr", F.col("duracion_hr").cast("double"))
    .withColumn("ingestion_time", F.current_timestamp())
)

#==========================
# 3. Escribir en Bronze (Delta)
#==========================

vuelos_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")

aeronaves_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")

aeropuertos_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")

mantenimientos_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos")

print("Bronze ingestion completada")
print(f"Tablas creadas: {CATALOG}.{BRONZE_SCHEMA}.vuelos, aeronaves, aeropuertos, mantenimientos")

