# Databricks notebook source
from pyspark.sql.functions import *
from functools import reduce
import os

# Configuración de acceso segura mediante variables de entorno
DATALAKE_KEY = os.getenv("DATALAKE_KEY")  
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_URL = "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits"

# 1. Configuración de acceso a Data Lake
spark.conf.set("fs.azure.account.key.kaitsdatalake.dfs.core.windows.net", DATALAKE_KEY)

# 2. Lectura del archivo CSV desde ADLS
df_custdata = spark.read.option("header", True).csv(
    "abfss://kaits@kaitsdatalake.dfs.core.windows.net/inbox/CustomerData.csv"
)

# 3. Lectura de DimGeography
df_geo = spark.read.format("jdbc") \
    .option("url", SQL_URL) \
    .option("dbtable", "DimGeography") \
    .option("user", SQL_USER) \
    .option("password", SQL_PASSWORD) \
    .load()

# 4. Lectura de DimCustomer
df_cus = spark.read.format("jdbc") \
    .option("url", SQL_URL) \
    .option("dbtable", "DimCustomer") \
    .option("user", SQL_USER) \
    .option("password", SQL_PASSWORD) \
    .load()

df_cus_alias = df_cus.select(*[col(c).alias("cus_" + c) for c in df_cus.columns])

# 5. Limpieza de datos
df_custdata_clean = df_custdata \
    .withColumn("BirthDate", to_date(col("BirthDate"), "yyyy-MM-dd")) \
    .withColumn("DateFirstPurchase", to_date(col("DateFirstPurchase"), "yyyy-MM-dd")) \
    .withColumn("YearlyIncome", col("YearlyIncome").cast("decimal(19,4)")) \
    .withColumn("TotalChildren", col("TotalChildren").cast("smallint")) \
    .withColumn("NumberChildrenAtHome", col("NumberChildrenAtHome").cast("smallint")) \
    .withColumn("NumberCarsOwned", col("NumberCarsOwned").cast("smallint"))

# 6. Join con Geografía
df_joined = df_custdata_clean.join(
    df_geo.select("City", "CountryRegionCode", "GeographyKey"),
    on=["City", "CountryRegionCode"],
    how="left"
)

# 7. Nuevos clientes
df_nuevo = df_joined.join(
    df_cus.select("CustomerAlternateKey").distinct(),
    on="CustomerAlternateKey",
    how="left_anti"
).dropDuplicates(["CustomerAlternateKey"])

# 8. Modificados vs sin cambios
df_merged = df_joined.join(
    df_cus_alias,
    df_joined["CustomerAlternateKey"] == df_cus_alias["cus_CustomerAlternateKey"],
    how="inner"
)
df_changes = df_merged.filter(col("cus_EmailAddress").isNotNull())

columnas_validas_sqlserver = [
    "GeographyKey", "CustomerAlternateKey", "FirstName", "MiddleName", "LastName",
    "BirthDate", "MaritalStatus", "Suffix", "Gender", "EmailAddress",
    "YearlyIncome", "TotalChildren", "NumberChildrenAtHome",
    "EnglishEducation", "SpanishEducation", "FrenchEducation",
    "EnglishOccupation", "SpanishOccupation", "FrenchOccupation",
    "HouseOwnerFlag", "NumberCarsOwned", "AddressLine1", "AddressLine2",
    "Phone", "DateFirstPurchase", "CommuteDistance"
]

condiciones_diferencia = reduce(
    lambda acc, name: acc | (col(name) != col("cus_" + name)),
    columnas_validas_sqlserver[1:],
    col(columnas_validas_sqlserver[0]) != col("cus_" + columnas_validas_sqlserver[0])
)

df_actualizaciones = df_changes.filter(condiciones_diferencia).dropDuplicates(["CustomerAlternateKey"])
df_unchanged = df_changes.filter(~condiciones_diferencia).dropDuplicates(["CustomerAlternateKey"])

# 9. Guardar nuevos en DimCustomer
df_nuevo_clean = df_nuevo.select(columnas_validas_sqlserver)
df_nuevo_clean.write.format("jdbc") \
    .option("url", SQL_URL) \
    .option("dbtable", "DimCustomer") \
    .option("user", SQL_USER) \
    .option("password", SQL_PASSWORD) \
    .mode("append").save()

# 10. Guardar actualizaciones en staging
df_actualizaciones_clean = df_actualizaciones.select(columnas_validas_sqlserver)
df_actualizaciones_clean.write.format("jdbc") \
    .option("url", SQL_URL) \
    .option("dbtable", "StgCustomerUpdates") \
    .option("user", SQL_USER) \
    .option("password", SQL_PASSWORD) \
    .mode("overwrite").save()

# 11. CustomerTracking
df_nuevos_tracking = df_nuevo_clean.withColumn("ActionType", lit("Nuevo"))
df_actualizaciones_tracking = df_actualizaciones_clean.withColumn("ActionType", lit("Modificado"))
df_sin_cambios_tracking = df_unchanged.select(columnas_validas_sqlserver).withColumn("ActionType", lit("Sin cambios"))

df_tracking = df_nuevos_tracking.unionByName(df_actualizaciones_tracking).unionByName(df_sin_cambios_tracking)

df_tracking.write.format("jdbc") \
    .option("url", SQL_URL) \
    .option("dbtable", "CustomerTracking") \
    .option("user", SQL_USER) \
    .option("password", SQL_PASSWORD) \
    .mode("overwrite").save()

# 12. Logs
print("Total de registros en CSV:", df_custdata.count())
print("Nuevos:", df_nuevo_clean.count())
print("Modificados:", df_actualizaciones_clean.count())
print("Sin cambios:", df_sin_cambios_tracking.count())
print("Total Tracking:", df_tracking.count())
