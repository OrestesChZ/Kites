# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("fs.azure.account.key.kaitsdatalake.dfs.core.windows.net", "Erxr+cJylGm/WFdLY5ExRGCc1kIpmKWCChPb2bLAUJiPMYpda4a4B/6m4Li6uZ3HVn5O59d2uBtX+AStxhXA6w==")


# COMMAND ----------

df_custdata = spark.read.option("header", True).csv(
    "abfss://kaits@kaitsdatalake.dfs.core.windows.net/inbox/CustomerData.csv"
)


# COMMAND ----------

df_custdata.count()

# COMMAND ----------

df_custdata.display()

# COMMAND ----------

df_custdata.printSchema()

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://kaitssql.database.windows.net:1433;database=Kaits"

db_properties = {
    "user": "adminochz",
    "password": "ores20tes00@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

df_geo = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits")
    .option("dbtable", "DimGeography")
    .option("user", "adminochz@kaitssqlserver")
    .option("password", "ores20tes00@")
    .load()
)


# COMMAND ----------

df_cus = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits")
    .option("dbtable", "DimCustomer")
    .option("user", "adminochz@kaitssqlserver")
    .option("password", "ores20tes00@")
    .load()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD
# MAGIC

# COMMAND ----------

from functools import reduce

# 1. Limpieza de df_custdata
df_custdata_clean = df_custdata \
    .withColumn("BirthDate", to_date(col("BirthDate"), "yyyy-MM-dd")) \
    .withColumn("DateFirstPurchase", to_date(col("DateFirstPurchase"), "yyyy-MM-dd")) \
    .withColumn("YearlyIncome", col("YearlyIncome").cast("decimal(19,4)")) \
    .withColumn("TotalChildren", col("TotalChildren").cast("smallint")) \
    .withColumn("NumberChildrenAtHome", col("NumberChildrenAtHome").cast("smallint")) \
    .withColumn("NumberCarsOwned", col("NumberCarsOwned").cast("smallint"))

# 2. Join con DimGeography
df_joined = df_custdata_clean.join(
    df_geo.select("City", "CountryRegionCode", "GeographyKey"),
    on=["City", "CountryRegionCode"],
    how="left"
)

# 3. Leer DimCustomer actual desde SQL Server
df_cus = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits") \
    .option("dbtable", "DimCustomer") \
    .option("user", "adminochz@kaitssqlserver") \
    .option("password", "ores20tes00@") \
    .load()

df_cus_alias = df_cus.select(
    *[col(c).alias("cus_" + c) for c in df_cus.columns]
)

# 4. Detectar nuevos clientes
df_nuevo = df_joined.join(
    df_cus.select("CustomerAlternateKey").distinct(),
    on="CustomerAlternateKey",
    how="left_anti"
).dropDuplicates(["CustomerAlternateKey"])

# 5. Preparar join para comparar con registros existentes
df_merged = df_joined.join(
    df_cus_alias,
    df_joined["CustomerAlternateKey"] == df_cus_alias["cus_CustomerAlternateKey"],
    how="inner"
)

df_changes = df_merged.filter(col("cus_EmailAddress").isNotNull())

# 6. Comparación estricta por columnas
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

# 7. Guardar NUEVOS en DimCustomer
df_nuevo_clean = df_nuevo.select(columnas_validas_sqlserver)
df_nuevo_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits") \
    .option("dbtable", "DimCustomer") \
    .option("user", "adminochz@kaitssqlserver") \
    .option("password", "ores20tes00@") \
    .mode("append") \
    .save()

# 8. Guardar MODIFICADOS en tabla staging
df_actualizaciones_clean = df_actualizaciones.select(columnas_validas_sqlserver)
df_actualizaciones_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits") \
    .option("dbtable", "StgCustomerUpdates") \
    .option("user", "adminochz@kaitssqlserver") \
    .option("password", "ores20tes00@") \
    .mode("overwrite") \
    .save()

# 9. Clasificación para CustomerTracking
df_nuevos_tracking = df_nuevo_clean.withColumn("ActionType", lit("Nuevo"))
df_actualizaciones_tracking = df_actualizaciones_clean.withColumn("ActionType", lit("Modificado"))
df_sin_cambios_tracking = df_unchanged.select(columnas_validas_sqlserver).withColumn("ActionType", lit("Sin cambios"))

df_tracking = df_nuevos_tracking.unionByName(df_actualizaciones_tracking).unionByName(df_sin_cambios_tracking)

df_tracking.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://kaitssqlserver.database.windows.net:1433;database=kaits") \
    .option("dbtable", "CustomerTracking") \
    .option("user", "adminochz@kaitssqlserver") \
    .option("password", "ores20tes00@") \
    .mode("overwrite") \
    .save()


# COMMAND ----------

print("Total de registros en CSV:", df_custdata.count())
print("Nuevos:", df_nuevo_clean.count())
print("Modificados:", df_actualizaciones_clean.count())
print("Sin cambios:", df_sin_cambios_tracking.count())
print("Total Tracking:", df_tracking.count())
