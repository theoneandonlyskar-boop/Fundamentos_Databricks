from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# =========================
# Configuración
# =========================
CATALOG = "aerolinea"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark = SparkSession.builder.getOrCreate()

# =========================
# 0. Asegurar schema Silver
# =========================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# =========================
# 1. Leer Bronze
# =========================

vuelos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")
aeronaves_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")
aeropuertos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")
mantenimientos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos")

# =========================
# 2. Silver vuelos
# =========================
#Transformaciones Silver vuelos
vuelos_slv = (
    vuelos_bz
    .select("vuelo_id", 
            "fecha",
            "origen_id", 
            "destino_id", 
            "aeronave_id", 
            "estado", 
            "duracion_min"
    )
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("duracion_min", F.col("duracion_min").cast("int"))
    .dropDuplicates(["vuelo_id"])
)

#Trasformaciones Silver aeronaves
aeronaves_slv = (
    aeronaves_bz
    .select("aeronave_id", 
            "modelo", 
            "fabricante", 
            "anio_fabricacion"
    )
    .withColumn("anio_fabricacion", F.col("anio_fabricacion").cast("int"))
    .dropDuplicates(["aeronave_id"])
)

#Trasformaciones Silver aeropuertos
aeropuertos_slv = (
    aeropuertos_bz
    .select("aeropuerto_id", 
            "nombre", 
            "ciudad", 
            "pais", 
            "lat", 
            "lon"
    )
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .dropDuplicates(["aeropuerto_id"])
)

#Trasformaciones Silver mantenimientos
mantenimientos_slv = (
    mantenimientos_bz
    .select("mantenimiento_id", 
            "aeronave_id", 
            "fecha", 
            "tipo", 
            "costo_usd", 
            "duracion_hr"
    )
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("costo_usd", F.col("costo_usd").cast("double"))
    .withColumn("duracion_hr", F.col("duracion_hr").cast("double"))
    .dropDuplicates(["mantenimiento_id"])
)

# =========================
# 3. Validaciónes de Calidad
# =========================
print("Nulos vuelo_id:", vuelos_slv.filter(F.col("vuelo_id").isNull()).count())

print("Duplicados vuelos:", vuelos_slv.groupBy("vuelo_id").count().filter(F.col("count") > 1).count())

# =========================
# 4. Guardar en Silver
# =========================

vuelos_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")

aeronaves_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")

aeropuertos_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")

mantenimientos_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

print("Silver Delta completada")
print(f"Tablas creadas/actualizadas: {CATALOG}.{SILVER_SCHEMA}.vuelos, aeronaves, aeropuertos, mantenimientos")

