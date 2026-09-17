"""
Taller 1 - ETL con PySpark (Online Retail Dataset)
====================================================
Este script realiza un proceso ETL completo sobre el dataset "Online Retail"
(UCI Machine Learning Repository) y responde a las 10 preguntas de análisis
solicitadas en el taller.

Cada pregunta/análisis genera SU PROPIO archivo CSV dentro de outputs/,
por ejemplo:
    outputs/01_total_facturas.csv
    outputs/02_clientes_unicos.csv
    outputs/03_ingreso_total.csv
    ...

Cómo ejecutarlo:
    python etl_pyspark.py

Requisitos:
    - Descargar el dataset (ver descargar_dataset.py)
    - Colocarlo en: ./data/OnlineRetail.csv
"""

import os
import sys

# -------------------------------------------------------------------
# IMPORTANTE (Windows): Spark lanza procesos internos de Python para
# ejecutar tareas (workers). En Windows, si solo se le dice "python",
# a veces resuelve contra el alias roto de la Microsoft Store en vez
# del Python real, y las tareas truenan con
# "SocketTimeoutException: Timed out while waiting for the Python
# worker to connect back". Por eso fijamos aquí, de forma explícita,
# la ruta completa al Python que está ejecutando este script
# (sys.executable) ANTES de crear la SparkSession.
# -------------------------------------------------------------------
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------
# 0. CONFIGURACIÓN GENERAL
# -----------------------------------------------------------------------
INPUT_PATH = "data/OnlineRetail.csv"
OUTPUT_DIR = "outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Taller1_ETL_OnlineRetail")
    .config("spark.sql.session.timeZone", "UTC")
    # Aumentamos el timeout de conexión del worker de Python como
    # margen de seguridad adicional en máquinas Windows más lentas.
    .config("spark.python.worker.connectionTimeout", "120s")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def guardar_csv(df, nombre):
    """
    Guarda un DataFrame como un único archivo CSV (con encabezado) dentro
    de OUTPUT_DIR, uno por análisis.

    Nota: usamos toPandas() + pandas.to_csv() en vez de df.write.csv(),
    porque en Windows Spark necesita winutils.exe/HADOOP_HOME instalado
    para escribir archivos, y con pandas evitamos ese problema. Como cada
    resultado es una agregación pequeña, no hay riesgo de memoria.
    """
    ruta = os.path.join(OUTPUT_DIR, f"{nombre}.csv")
    pdf = df.toPandas()
    pdf.to_csv(ruta, index=False, encoding="utf-8")
    print(f"-> Guardado: {ruta}  ({len(pdf)} filas)")


# -----------------------------------------------------------------------
# 1. LECTURA DE DATOS (Extract)
# -----------------------------------------------------------------------
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "ISO-8859-1")
    .load(INPUT_PATH)
)

print("Esquema original:")
df_raw.printSchema()
print(f"Filas leídas: {df_raw.count()}")

# -----------------------------------------------------------------------
# 2. TRANSFORMACIÓN / LIMPIEZA (Transform)
# -----------------------------------------------------------------------

# 2.1 Selección de columnas relevantes (select)
df = df_raw.select(
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country"
)

# 2.2 Conversión de tipos
df = (
    df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    .withColumn("CustomerID", F.col("CustomerID").cast("int"))
)

# 2.3 Filtrado de datos (filter / where)
df_clean = (
    df.filter(F.col("CustomerID").isNotNull())
    .filter(F.col("Description").isNotNull())
    .where(F.col("UnitPrice").isNotNull())
    .where(F.col("Quantity").isNotNull())
)

# 2.4 Columnas derivadas (withColumn)
df_clean = (
    df_clean
    .withColumn("TotalPrice", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    .withColumn("IsReturn", F.when(F.col("Quantity") < 0, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("InvoiceYear", F.year("InvoiceDate"))
    .withColumn("InvoiceMonth", F.month("InvoiceDate"))
    .withColumn("YearMonth", F.date_format("InvoiceDate", "yyyy-MM"))
)

df_clean.cache()
print(f"Filas después de limpieza: {df_clean.count()}")

# 2.5 Ordenamiento (orderBy) - ejemplo: las 10 ventas de mayor valor
top_ventas = df_clean.orderBy(F.col("TotalPrice").desc()).limit(10)
guardar_csv(top_ventas, "00_top10_ventas_valor")


# =========================================================================
# PREGUNTAS DE ANÁLISIS (una pregunta = un archivo CSV)
# =========================================================================

# -------------------------------------------------------------------
# 1. Número total de facturas en el dataset
# -------------------------------------------------------------------
total_facturas = df_clean.select("InvoiceNo").distinct().count()
print(f"\n1. Número total de facturas: {total_facturas}")

df_p1 = spark.createDataFrame([("Total facturas", total_facturas)], ["Metrica", "Valor"])
guardar_csv(df_p1, "01_total_facturas")

# -------------------------------------------------------------------
# 2. Número de clientes únicos
# -------------------------------------------------------------------
clientes_unicos = df_clean.select("CustomerID").distinct().count()
print(f"2. Número de clientes únicos: {clientes_unicos}")

df_p2 = spark.createDataFrame([("Clientes únicos", clientes_unicos)], ["Metrica", "Valor"])
guardar_csv(df_p2, "02_clientes_unicos")

# -------------------------------------------------------------------
# 3. Ingreso total (Quantity * UnitPrice)
#    Se calcula solo sobre ventas positivas (se excluyen devoluciones)
# -------------------------------------------------------------------
ingreso_total = (
    df_clean.filter(F.col("Quantity") > 0)
    .agg(F.sum("TotalPrice").alias("IngresoTotal"))
    .collect()[0]["IngresoTotal"]
)
print(f"3. Ingreso total (ventas positivas): {ingreso_total:.2f}")

df_p3 = spark.createDataFrame([("Ingreso total", round(ingreso_total, 2))], ["Metrica", "Valor"])
guardar_csv(df_p3, "03_ingreso_total")

# -------------------------------------------------------------------
# 4. Producto más vendido en cantidad (groupBy + agg + orderBy)
# -------------------------------------------------------------------
producto_top = (
    df_clean.filter(F.col("Quantity") > 0)
    .groupBy("StockCode", "Description")
    .agg(F.sum("Quantity").alias("CantidadTotal"))
    .orderBy(F.col("CantidadTotal").desc())
)
guardar_csv(producto_top.limit(20), "04_producto_mas_vendido")

# -------------------------------------------------------------------
# 5. Cliente con mayor volumen de compra en dinero
# -------------------------------------------------------------------
cliente_top = (
    df_clean.filter(F.col("Quantity") > 0)
    .groupBy("CustomerID", "Country")
    .agg(F.sum("TotalPrice").alias("TotalGastado"))
    .orderBy(F.col("TotalGastado").desc())
)
guardar_csv(cliente_top.limit(20), "05_cliente_mayor_compra")

# -------------------------------------------------------------------
# 6. Top 5 países que más compran fuera de Reino Unido
# -------------------------------------------------------------------
top_paises = (
    df_clean.filter(F.col("Quantity") > 0)
    .filter(F.col("Country") != "United Kingdom")
    .groupBy("Country")
    .agg(F.sum("TotalPrice").alias("TotalPais"))
    .orderBy(F.col("TotalPais").desc())
    .limit(5)
)
guardar_csv(top_paises, "06_top5_paises_sin_uk")

# -------------------------------------------------------------------
# 7. Ticket promedio por factura (promedio del total gastado por InvoiceNo)
# -------------------------------------------------------------------
total_por_factura = (
    df_clean.filter(F.col("Quantity") > 0)
    .groupBy("InvoiceNo")
    .agg(F.sum("TotalPrice").alias("TotalFactura"))
)
ticket_promedio = total_por_factura.agg(F.avg("TotalFactura").alias("TicketPromedio")).collect()[0]["TicketPromedio"]
print(f"7. Ticket promedio por factura: {ticket_promedio:.2f}")

df_p7 = spark.createDataFrame([("Ticket promedio", round(ticket_promedio, 2))], ["Metrica", "Valor"])
guardar_csv(df_p7, "07_ticket_promedio")

# -------------------------------------------------------------------
# 8. Mínimo, máximo y promedio de productos por factura
# -------------------------------------------------------------------
items_por_factura = (
    df_clean.groupBy("InvoiceNo")
    .agg(F.count("StockCode").alias("NumProductos"))
)
stats_items = items_por_factura.agg(
    F.min("NumProductos").alias("Min"),
    F.max("NumProductos").alias("Max"),
    F.avg("NumProductos").alias("Promedio"),
)
guardar_csv(stats_items, "08_stats_productos_por_factura")

# -------------------------------------------------------------------
# 9. Mes del año con más ventas
# -------------------------------------------------------------------
ventas_por_mes = (
    df_clean.filter(F.col("Quantity") > 0)
    .groupBy("InvoiceMonth")
    .agg(F.sum("TotalPrice").alias("TotalMes"))
    .orderBy(F.col("TotalMes").desc())
)
guardar_csv(ventas_por_mes, "09_ventas_por_mes")

# -------------------------------------------------------------------
# 10. Porcentaje de facturas con devoluciones (Quantity negativo)
# -------------------------------------------------------------------
facturas_con_devolucion = (
    df_clean.filter(F.col("Quantity") < 0)
    .select("InvoiceNo")
    .distinct()
    .count()
)
pct_devoluciones = (facturas_con_devolucion / total_facturas) * 100
print(f"10. % de facturas con devoluciones: {pct_devoluciones:.2f}%")

df_p10 = spark.createDataFrame(
    [("Facturas con devolución", float(facturas_con_devolucion)),
     ("Porcentaje (%)", round(pct_devoluciones, 2))],
    ["Metrica", "Valor"]
)
guardar_csv(df_p10, "10_pct_facturas_devoluciones")


# =========================================================================
# EXTRA: FUNCIONES DE VENTANA (opcional del taller)
# =========================================================================

# Ranking de clientes por gasto total (row_number y rank)
ventana_clientes = Window.orderBy(F.col("TotalGastado").desc())
ranking_clientes = (
    cliente_top
    .withColumn("Ranking", F.rank().over(ventana_clientes))
    .withColumn("Posicion", F.row_number().over(ventana_clientes))
    .limit(10)
)
guardar_csv(ranking_clientes, "11_ranking_clientes")

# Ranking de productos más vendidos por país (partición por país)
ventas_producto_pais = (
    df_clean.filter(F.col("Quantity") > 0)
    .groupBy("Country", "StockCode", "Description")
    .agg(F.sum("Quantity").alias("CantidadTotal"))
)
ventana_pais = Window.partitionBy("Country").orderBy(F.col("CantidadTotal").desc())
ranking_producto_pais = (
    ventas_producto_pais
    .withColumn("RankingEnPais", F.row_number().over(ventana_pais))
    .filter(F.col("RankingEnPais") <= 3)   # top 3 producto por país
    .orderBy("Country", "RankingEnPais")
)
guardar_csv(ranking_producto_pais, "12_top3_productos_por_pais")


# =========================================================================
# EXTRA: EJEMPLO DE JOIN
# =========================================================================
dim_paises = (
    df_clean.groupBy("Country")
    .agg(
        F.countDistinct("CustomerID").alias("ClientesPorPais"),
        F.sum("TotalPrice").alias("IngresoPorPais"),
    )
)
clientes_enriquecidos = (
    cliente_top.join(dim_paises, on="Country", how="left")
    .orderBy(F.col("TotalGastado").desc())
)
guardar_csv(clientes_enriquecidos.limit(20), "13_clientes_enriquecidos_join")


print("\n===== PROCESO ETL FINALIZADO. Revisa la carpeta 'outputs/' =====")
print("Se generó un archivo CSV por cada pregunta/análisis.")

spark.stop()