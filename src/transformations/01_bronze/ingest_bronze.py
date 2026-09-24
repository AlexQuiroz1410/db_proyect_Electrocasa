from pyspark import pipelines as pd
from pyspark.sql.functions import col, current_timestamp, lit
from src.schemas.bronze.ventas_sucursal import schema_ventas_sucursal
from src.schemas.bronze.resenas import schema_resena
from src.schemas.bronze.devoluciones import schema_devoluciones
from src.schemas.bronze.empleados_rrhh import schema_empleados
from src.schemas.bronze.productos import schema_productos

catalog = spark.conf.get("bundle.catalog")
schema_bronze = spark.conf.get("bundle.schema_bronze")
landing_path = spark.conf.get("bundle.landing_path")
schema_location = spark.conf.get("bundle.schema_location")

@pd.table(
    name=f"{catalog}.{schema_bronze}.brz_ventas_sucursales",
    comment="Ventas diarias por 40 sucursales",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.appendOnly": "true"
    }
)

def bronze_ventas_sucursales():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.schemaLocation",f"{schema_location}ventas/")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_ventas_sucursal())
        .load(f"{landing_path}/ventas/")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader

@pd.table(
    name=f"{catalog}.{schema_bronze}.brz_resenas",
    comment="Reseñas diarias de las 40 sucursales",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.appendOnly": "true"
    }
)

def bronze_resenas():
    df_reader = (
        spark.read
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("cloudFiles.schemaLocation",f"{schema_location}resenas/")
        .schema(schema_resena())
        .load(f"{landing_path}/resenas/")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader


@pd.table(
    name=f"{catalog}.{schema_bronze}.brz_devoluciones",
    comment="Devoluciones diarias por 40 sucursales",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.appendOnly": "true"
    }
)

def bronze_devoluciones():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.schemaLocation",f"{schema_location}devoluciones/")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_devoluciones())
        .load(f"{landing_path}/devoluciones/")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader    


@pd.table(
    name=f"{catalog}.{schema_bronze}.brz_empleados_rrhh",
    comment="Empleados",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.appendOnly": "true"
    }
)

def bronze_empleados():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.schemaLocation",f"{schema_location}empleados/")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_empleados())
        .load(f"{landing_path}/empleados/")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader 

@pd.materialized_view(
    name=f"{catalog}.{schema_bronze}.brz_productos",
    comment="Productos"
)

def bronze_productos():
    df_reader = (
        spark.read
        .format("json")
        .schema(schema_productos())
        .load(f"{landing_path}/productos/")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", lit("catalogo_productos"))
    )
    
    return df_reader

@pd.materialized_view(
    name=f"{catalog}.{schema_bronze}.brz_tracking",
    comment="Tracking desde Azure SQL"
)

def bronze_tracking():
    df_reader = (
        spark.read
        .format("jdbc")
        .option("url","jdbc:sqlserver://analyticsdmc.database.windows.net:1433;database=electrocasadb;encrypt=true;trustServerCertificate=false;loginTimeout=30;")
        .option("dbtable", "dbo.TrackingEnvios")
        .option("user","sqladmin")
        .option("password","mdp123$$")
        .load()
    )
    return (
            df_reader
            .select(
                col("tracking_id").cast("string"),
                col("pedido_id").cast("string"),
                col("courier").cast("string"),
                col("estado_entrega").cast("string"),
                col("sucursal_origen").cast("string"),
                col("fecha_actualizacion").cast("string")
            )
            .withColumn("ingestion_at",current_timestamp())
            .withColumn("source_system",lit("Azure SQL Database"))
    )


