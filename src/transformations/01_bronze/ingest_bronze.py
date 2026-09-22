from pyspark import pipelines as pd
from pyspark.sql.functions import col, current_timestamp, lit
from src.schemas.bronze.ventas_sucursal import schema_ventas_sucursal
from src.schemas.bronze.resenas import schema_resena
from src.schemas.bronze.devoluciones import schema_devoluciones
from src.schemas.bronze.empleados_rrhh import schema_empleados
from src.schemas.bronze.productos import schema_productos


@pd.table(
    name="dbelectrocasa.bronze.brz_ventas_sucursales",
    comment="Ventas diarias por 40 sucursales",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true"
    }
)

def bronze_ventas_sucursales():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_ventas_sucursal())
        .load("/Volumes/dbelectrocasa/default/vol_landing/electrocasa-data/ventas_sucursales.csv")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader

@pd.table(
    name="dbelectrocasa.bronze.brz_resenas",
    comment="Reseñas diarias de las 40 sucursales",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true"
    }
)

def bronze_resenas():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_resena())
        .load("/Volumes/dbelectrocasa/default/vol_landing/electrocasa-data/resenas_clientes.json")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader


@pd.table(
    name="dbelectrocasa.bronze.brz_devoluciones",
    comment="Devoluciones diarias por 40 sucursales",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true"
    }
)

def bronze_devoluciones():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_devoluciones())
        .load("/Volumes/dbelectrocasa/default/vol_landing/electrocasa-data/devoluciones.csv")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader    


@pd.table(
    name="dbelectrocasa.bronze.brz_empleados_rrhh",
    comment="Empleados",
    table_properties={  
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true"
    }
)

def bronze_empleados():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_empleados())
        .load("/Volumes/dbelectrocasa/default/vol_landing/electrocasa-data/empleados_rrhh.csv")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader 

@pd.materialized_view(
    name="dbelectrocasa.bronze.brz_productos",
    comment="Productos"
)

def bronze_productos():
    df_reader = (
        spark.read
        .format("json")
        .option("header",True)
        .option("delimiter",",")
        .schema(schema_productos())
        .load("/Volumes/dbelectrocasa/default/vol_landing/electrocasa-data/catalogo_productos.json")
        .withColumn("ingestion_at", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    
    return df_reader

@pd.materialized_view(
    name="dbelectrocasa.bronze.brz_tracking",
    comment="Tracking desde Azure SQL"
)

def bronze_tracking():
    df_reader = (
        spark.read
        .format("jdbc")
        .option("url","jdbc_sqlserver://analyticsdmc.database.windows.net:1433")
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
            .withColumn("source_system",lit("azure_sql_tracking"))
    )


