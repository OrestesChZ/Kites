# Kites

# Proyecto: Pipeline de Ingesta, Limpieza y Escritura de Datos de Clientes

Este pipeline orquesta la ingesta, transformación y persistencia de datos de clientes desde un archivo `.csv` alojado en un repositorio público, procesado en Databricks, y almacenado finalmente en Azure Data Lake.

---

## 🧩 Componentes del Pipeline: `pl_copy_git_to_datalake`

### 1. **Actividad: Copy to DataLake**
- **Tipo:** Copy
- **Origen:** Archivo CSV desde un repositorio HTTP (`dataset_git`)
- **Destino:** Azure Data Lake Gen2 (`dataset_datalake`)
- **Formato:** Delimited Text (`.txt`), con `quoteAllText = true`
- **Método HTTP:** GET
- **Uso:** Descarga e ingesta el archivo crudo desde la fuente externa al Data Lake (zona *inbox*).

---

### 2. **Actividad: NotebookDatabricks**
- **Tipo:** DatabricksNotebook
- **Notebook Ejecutado:** `/Users/oreschz@hotmail.com/KaitNotebook`
- **Dependencia:** Se ejecuta solo si `Copy to DataLake` fue exitoso.
- **Uso:** 
  - Limpieza y normalización de datos.
  - Conversión de tipos de datos.
  - Enriquecimiento con tabla de dimensiones (join con `DimGeography`).
  - Aplicación de lógica SCD  para sobreescribir la tabla `DimCustomer` vía JDBC.

---

### 3. **Actividad: copyProcessedData**
- **Tipo:** Copy
- **Origen:** Datos procesados por Databricks (`ds_customer_inbox`)
- **Destino:** Zona procesada en el Data Lake (`ds_customer_processed`)
- **Formato:** Delimited Text (`.txt`)
- **Comportamiento:** `FlattenHierarchy` para almacenar en estructura plana.
- **Uso:** Exporta los resultados finales procesados por Databricks nuevamente al Data Lake.

---

## 🛠️ Tecnologías Usadas

- **Azure Data Factory (ADF)**: Orquestación de flujos ETL.
- **Azure Data Lake Storage Gen2**: Almacenamiento estructurado.
- **Azure Databricks**: Transformación y limpieza con PySpark.
- **Azure SQL Database**: Almacén relacional destino (tabla `DimCustomer`).
- **PySpark**: Procesamiento distribuido de datos en Databricks.
- **JDBC**: Comunicación entre Spark y SQL Server para escritura final.

---

## ⚙️ Lógica del Notebook `KaitNotebook`

- Carga y limpieza de `CustomerData.csv`.
- Remoción de caracteres corruptos y acentos.
- Estándar de ciudades como `"münchen" → "muenchen"` antes de limpieza.
- Conversión de tipos (`String`, `Date`, `Double`, `Integer`, `Boolean`).
- Join con `DimGeography` usando `City` y `CountryRegionCode`.
- Generación de `df_final_to_upsert` sin duplicar `CustomerKey`.
- Escritura final a `DimCustomer` vía JDBC (`overwrite` mode).

---

## 🧪 Siguientes Pasos Sugeridos

1. **Automatización del Pipeline:**
   - **Triggers basados en eventos:** Configurar un trigger en Azure Data Factory que detecte la llegada de nuevos archivos al contenedor `inbox` (usando eventos de blob storage).
   - **Triggers programados:** Agendar ejecuciones recurrentes (ej. cada hora, diariamente) para mantener los datos sincronizados automáticamente.

2. **Optimización de Escritura en DimCustomer (SCD Tipo 1):**
   - Reemplazar el `.mode("overwrite")` por una estrategia más segura como `merge` o `upsert`, evitando la pérdida de registros históricos y minimizando riesgos en ambientes productivos.
   - Implementar una comparación de registros basada en claves naturales (`CustomerAlternateKey`, `EmailAddress`, etc.) y columnas críticas para detectar cambios.

3. **Logging y Auditoría:**
   - Incorporar mecanismos de logging (mediante tablas de auditoría o Azure Monitor) para rastrear ejecuciones, errores y tiempos de carga.
   - Registrar el número de filas insertadas, actualizadas y descartadas.

4. **Validaciones Automáticas:**
   - Añadir pasos posteriores que verifiquen que no hay valores nulos en claves primarias (`GeographyKey`, `CustomerKey`).
   - Detectar y alertar sobre registros sin match con `DimGeography` para corrección de calidad de datos.

5. **Escalabilidad y Modularización:**
   - Parametrizar el pipeline para facilitar su uso con otros datasets (reutilizar el mismo flujo para diferentes orígenes).
   - Separar la lógica de limpieza, transformación y escritura en notebooks independientes y reutilizables.

---