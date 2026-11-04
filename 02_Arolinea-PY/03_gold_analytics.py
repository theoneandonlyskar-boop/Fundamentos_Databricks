# 03_gold_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# Configuración
# =========================
CATALOG = "aerolinea"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark = SparkSession.builder.getOrCreate()

# =========================
# 0. Asegurar schema Gold
# =========================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# =========================
# 1. Leer Silver
# =========================
vuelos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
aeronaves_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
aeropuertos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
mantenimientos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

# =========================
# 2. Vuelos enriquecidos
# =========================
# Dos copias de aeropuertos: una para el origen y otra para el destino
origen = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("origen_id"),
    F.col("nombre").alias("origen_nombre"),
    F.col("ciudad").alias("origen_ciudad"),
    F.col("pais").alias("origen_pais"),
)
destino = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("destino_id"),
    F.col("nombre").alias("destino_nombre"),
    F.col("ciudad").alias("destino_ciudad"),
    F.col("pais").alias("destino_pais"),
)

vuelos_gold = (
    vuelos_slv
    .withColumn("estado", F.lower(F.trim(F.col("estado"))))
    .join(aeronaves_slv, "aeronave_id", "left")
    .join(origen, "origen_id", "left")
    .join(destino, "destino_id", "left")
    .withColumn("mes", F.trunc("fecha", "month"))
    .withColumn("edad_aeronave", F.year("fecha") - F.col("anio_fabricacion"))
    .withColumn("operado", F.col("estado") != "cancelado")
    .withColumn("ruta_por_revisar", F.col("origen_id") == F.col("destino_id"))
)

# =========================
# 3. Mantenimientos enriquecidos
# =========================
mantenimientos_gold = (
    mantenimientos_slv
    .join(aeronaves_slv, "aeronave_id", "left")
    .withColumn("mes", F.trunc("fecha", "month"))
    .withColumn("edad_aeronave", F.year("fecha") - F.col("anio_fabricacion"))
)

# =========================
# 4. Validación: la unión no debe multiplicar ni perder filas
# =========================
assert vuelos_gold.count() == vuelos_slv.count(), "La unión multiplicó o perdió vuelos."
assert mantenimientos_gold.count() == mantenimientos_slv.count(), "La unión multiplicó o perdió mantenimientos."
print("Vuelos enriquecidos:", vuelos_gold.count())
print("Mantenimientos enriquecidos:", mantenimientos_gold.count())

# =========================
# 5. KPI de puntualidad
# =========================
es_operado = F.when(F.col("operado"), 1).otherwise(0)
a_tiempo = F.when(F.col("estado") == "a_tiempo", 1).otherwise(0)
retrasado = F.when(F.col("estado") == "retrasado", 1).otherwise(0)
cancelado = F.when(F.col("estado") == "cancelado", 1).otherwise(0)

kpi_puntualidad = (
    vuelos_gold
    .groupBy("modelo", "fabricante", "origen_pais", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),
        F.sum(es_operado).alias("vuelos_operados"),
        F.sum(a_tiempo).alias("vuelos_a_tiempo"),
        F.sum(retrasado).alias("vuelos_retrasados"),
        F.sum(cancelado).alias("vuelos_cancelados"),
        F.round(F.avg(F.when(F.col("operado"), F.col("duracion_min"))), 2)
         .alias("duracion_promedio_operados_min"),
        F.sum(F.when(F.col("ruta_por_revisar"), 1).otherwise(0)).alias("rutas_por_revisar"),
    )
    .withColumn(
        "puntualidad_operados_pct",
        F.when(F.col("vuelos_operados") > 0,
               F.round(100.0 * F.col("vuelos_a_tiempo") / F.col("vuelos_operados"), 2)),
    )
    .withColumn(
        "cancelacion_pct",
        F.round(100.0 * F.col("vuelos_cancelados") / F.col("total_vuelos"), 2),
    )
)

# =========================
# 6. KPI de costo de mantenimiento
# =========================
kpi_costo = (
    mantenimientos_gold
    .groupBy("aeronave_id", "modelo", "fabricante", "tipo", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.round(F.sum("costo_usd"), 2).alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.round(F.sum("duracion_hr"), 2).alias("duracion_total_hr"),
        F.round(F.avg("duracion_hr"), 2).alias("duracion_promedio_hr"),
    )
)

# Validación: el costo total debe conservarse al agrupar
costo_silver = mantenimientos_slv.agg(F.round(F.sum("costo_usd"), 2)).first()[0]
costo_kpi = kpi_costo.agg(F.round(F.sum("costo_total_usd"), 2)).first()[0]
print("Costo Silver:", costo_silver, "| Costo KPI:", costo_kpi)

# =========================
# 7. Actividad y costo por aeronave y mes + ranking
# =========================
actividad = (
    vuelos_gold.groupBy("aeronave_id", "mes")
    .agg(
        F.count("*").alias("vuelos"),
        F.sum(es_operado).alias("vuelos_operados"),
    )
)
costos = (
    mantenimientos_gold.groupBy("aeronave_id", "mes")
    .agg(
        F.count("*").alias("mantenimientos"),
        F.round(F.sum("costo_usd"), 2).alias("costo_total_usd"),
    )
)

ventana = Window.partitionBy("mes").orderBy(F.desc("costo_total_usd"))

resumen_aeronave_mes = (
    actividad.join(costos, ["aeronave_id", "mes"], "full_outer")
    .fillna(0, subset=["vuelos", "vuelos_operados", "mantenimientos", "costo_total_usd"])
    .join(aeronaves_slv.select("aeronave_id", "modelo", "fabricante"), "aeronave_id", "left")
    .withColumn(
        "costo_por_vuelo_operado",
        F.when(F.col("vuelos_operados") > 0,
               F.round(F.col("costo_total_usd") / F.col("vuelos_operados"), 2)),
    )
    .withColumn("ranking_costo_mes", F.dense_rank().over(ventana))
)

# =========================
# 8. Guardar en Gold
# =========================
tablas = {
    "vuelos_enriquecidos": vuelos_gold,
    "mantenimientos_enriquecidos": mantenimientos_gold,
    "kpi_puntualidad_vuelos": kpi_puntualidad,
    "kpi_costo_mantenimiento": kpi_costo,
    "resumen_aeronave_mes": resumen_aeronave_mes,
}

for nombre, df in tablas.items():
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", True)
       .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.{nombre}"))

print("Gold completada")
print(f"Tablas creadas/actualizadas en {CATALOG}.{GOLD_SCHEMA}: {', '.join(tablas)}")
