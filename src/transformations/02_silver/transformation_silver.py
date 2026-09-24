from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, lit, to_date, current_timestamp, trim, initcap, when, regexp_replace, posexplode, to_json, struct
from pyspark.sql.types import StringType,IntegerType,DecimalType, DateType, TimestampType

catalog = spark.conf.get("bundle.catalog")
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

@dp.table(
    name=f"{catalog}.{schema_silver}.slv_ventas"
)

@dp.expect_or_drop("monto_total_valido", "monto_total IS NOT NULL AND monto_total > 0")

def slv_ventas():
    df_transformation = spark.readStream.table(f"{catalog}.{schema_bronze}.brz_ventas_sucursales")
    df_unicas = (
        df_transformation.dropDuplicates(["venta_id"])
        .withColumn("metodo_pago",initcap(trim(col("metodo_pago"))))
        .withColumn(
            "metodo_pago",
            when(col("metodo_pago") == "Efv","Efectivo")
            .when(col("metodo_pago") == "Tarjeta","Tarjeta De Credito")
            .when(col("metodo_pago") == "Tarjeta_Credito","Tarjeta De Credito")
            .when(col("metodo_pago") == "Tc","Tarjeta De Credito")
            .when(col("metodo_pago") == "Transferencia","Transferencia Bancaria")
            .otherwise(col("metodo_pago"))
        )
        .withColumn("fecha_venta",to_date(col("fecha_venta")))
        .withColumn("sucursal_id",coalesce(col("sucursal_id"),lit("Sin Sucursal")))
        .withColumn("updated_at", current_timestamp())
    )
    return (
        df_unicas
        .select(
            col("venta_id").cast(StringType()),
            col("sucursal_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("cantidad").cast(IntegerType()),
            col("monto_total").cast(DecimalType(12,2)),
            col("metodo_pago").cast(StringType()),
            col("fecha_venta"),
            col("canal").cast(StringType()),
            col("ingestion_at"),
            col("source_file"),
            col("updated_at")
        )
    )


@dp.table(
    name=f"{catalog}.{schema_silver}.slv_devoluciones"
)

@dp.expect_or_drop("monto_reembolso_valido", "monto_reembolso IS NOT NULL AND monto_reembolso > 0")

def slv_devoluciones():
    df_transformation = spark.readStream.table(f"{catalog}.{schema_bronze}.brz_devoluciones")
    df_unicas = (
        df_transformation.dropDuplicates(["devolucion_id"])
        .withColumn("motivo",initcap(trim(regexp_replace(col("motivo"),"_"," "))))
        .withColumn("fecha_devolucion",to_date(col("fecha_devolucion")))
        .withColumn("pedido_id",coalesce(col("pedido_id"),lit("Sin Pedido")))
        .withColumn("updated_at", current_timestamp())
    )
    return (
        df_unicas
        .select(
            col("devolucion_id").cast(StringType()),
            col("pedido_id").cast(StringType()),
            col("sucursal_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("motivo").cast(StringType()),
            col("monto_reembolso").cast(DecimalType(12,2)),
            col("fecha_devolucion"),
            col("ingestion_at"),
            col("source_file"),
            col("updated_at")
        )
    )

@dp.table(
    name=f"{catalog}.{schema_silver}.slv_resenas"
)

@dp.expect_or_drop("calificacion_valida","calificacion BETWEEN 1 AND 5 AND calificacion IS NOT NULL")
@dp.expect_all({"fecha_resena_informado":"fecha_resena IS NOT NULL"})

def slv_resenas():
    df_transformation = spark.readStream.table(f"{catalog}.{schema_bronze}.brz_resenas")
    df_unicas = (
        df_transformation
        .dropDuplicates(["resena_id"])
        .fillna(
            {
                "comentario":"Sin Comentario"
            }
        )
        .withColumn("updated_at", current_timestamp())
    )
    
    return (
        df_unicas
        .select(
            col("resena_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("cliente_id").cast(StringType()),
            col("calificacion").cast(IntegerType()),
            col("comentario").cast(StringType()),
            col("tags"),
            col("respuestas"),
            col("fecha_resena").cast(DateType()),
            col("ingestion_at"),
            col("source_file"),
            col("updated_at")
        )
    )


@dp.table(
    name=f"{catalog}.{schema_silver}.slv_resenas_detalle"
)

def slv_resenas_detalle():
    df_transformation = spark.readStream.table(f"{catalog}.{schema_bronze}.brz_resenas")
    df_unicas = (
        df_transformation
        .dropDuplicates(["resena_id"])
        .fillna(
            {
                "comentario":"Sin Comentario"
            }
        )
    ).withColumn("updated_at", current_timestamp())

    df_respuestas_posexplode = df_unicas.select(
        "resena_id",
        posexplode("respuestas").alias("pos", "respuesta"),"ingestion_at","source_file","updated_at"
    ).select(
        "resena_id",
        col("pos").alias("pos_respuesta"),
        col("respuesta.autor").alias("autor"),
        col("respuesta.texto").alias("texto"),
        col("ingestion_at"),
        col("source_file"),
        col("updated_at")
    )
    
    return (
        df_respuestas_posexplode
        .select(
            col("resena_id").cast(StringType()),
            col("pos_respuesta").cast(IntegerType()),
            col("autor").cast(StringType()),
            col("texto").cast(StringType()),
            col("ingestion_at"),
            col("source_file"),
            col("updated_at")
        )
    )


@dp.table(
    name=f"{catalog}.{schema_silver}.slv_productos"
)

@dp.expect_or_drop("precio_valido","""CAST(precio_lista AS DOUBLE) IS NOT NULL AND CAST(precio_lista AS DOUBLE) > 0""")

def slv_productos():
    df_transformation = spark.read.table(f"{catalog}.{schema_bronze}.brz_productos")
    df_unicas_transformation = (
        df_transformation.dropDuplicates(["producto_id"])
        .withColumn("categoria",initcap(trim(col("categoria"))))
        .withColumn(
            "categoria",
            when(col("categoria") == "Climatización","Climatizacion")
            .when(col("categoria") == "Electrónica","Electronica")
            .when(col("categoria") == "Linea_Blanca","Linea Blanca")
            .when(col("categoria") == "Línea Blanca","Linea Blanca")
            .otherwise(col("categoria"))
        )
        .withColumn("marca",coalesce(col("marca"),lit("Sin Marca")))
        .withColumn("updated_at",current_timestamp())
    )
    return (
        df_unicas_transformation
        .select(
            col("producto_id").cast(StringType()),
            col("nombre_producto").cast(StringType()),
            col("categoria").cast(StringType()),
            col("marca").cast(StringType()),
            col("precio_lista").cast(DecimalType(12,2)),
            col("ingestion_at"),
            col("source_file"),
            col("updated_at")
        )
    )

@dp.temporary_view(
    name="view_empleados",
    comment="Vista limpia de empleados"
)

@dp.expect_or_drop("dni_valido","dni IS NOT NULL")
@dp.expect_or_drop("fecha_evento_validacion","fecha_evento IS NOT NULL")

def staging_empleados():
    df_transformation = spark.readStream.table(f"{catalog}.{schema_bronze}.brz_empleados_rrhh")
    df_estandarizado = (
        df_transformation
        .withColumn("tipo_evento",initcap(trim(regexp_replace(col("tipo_evento"),"_"," "))))
        .withColumn("email",coalesce(col("email"),lit("Sin Correo")))
        .withColumn("updated_at", current_timestamp())
        .select(
            col("id_empleado").cast(StringType()),
            col("nombre").cast(StringType()),
            col("dni").cast(StringType()),
            col("email").cast(StringType()),
            col("salario").cast(DecimalType(12,2)),
            col("sucursal_id").cast(StringType()),
            col("cargo").cast(StringType()),
            col("tipo_evento").cast(StringType()),
            col("fecha_evento").cast(TimestampType()),
            col("ingestion_at"),
            col("source_file"),
            col("updated_at")
        )
    )
    return df_estandarizado

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.slv_empleados_hist",
    comment="Lista de Empleados"
)

dp.create_auto_cdc_flow(
    name="empleados_cdc_type2",
    source= "view_empleados",
    target= f"{catalog}.{schema_silver}.slv_empleados_hist",
    keys=["id_empleado"],
    sequence_by="fecha_evento",
    column_list = [
        "id_empleado","nombre","dni","email","salario","sucursal_id","cargo","tipo_evento","fecha_evento"
        ],
    stored_as_scd_type="2"
)

@dp.table(
    name=f"{catalog}.{schema_silver}.slv_empleados_actual"
)    

def slv_empleados_actual():
    df_data = spark.read.table(f"{catalog}.{schema_silver}.slv_empleados_hist")
    df_actual = (
        df_data.filter(
            col("__END_AT").isNull()
            & (col("tipo_evento") != "Baja")
        )
    )

    return df_actual


@dp.table(
    name=f"{catalog}.{schema_silver}.slv_tracking"
)

@dp.expect_or_drop("estado_entrega_valido","""estado_entrega IN ('En Transito','Pendiente','Entregado','Devuelto')""")
@dp.expect_all({"fecha_actualizacion_informado":"fecha_actualizacion IS NOT NULL"})

def slv_tracking():
    df_transformation = spark.read.table(f"{catalog}.{schema_bronze}.brz_tracking")
    df_unicas_transformation = (
        df_transformation.dropDuplicates(["tracking_id"])
        .withColumn("courier",initcap(trim(col("courier"))))
        .withColumn("estado_entrega",initcap(trim(regexp_replace(col("estado_entrega"),"_"," "))))
        .withColumn(
            "estado_entrega",
            when(col("estado_entrega") == "En Camino","En Transito")
            .otherwise(col("estado_entrega"))
        )
        .withColumn("updated_at",current_timestamp())
    )
    return (
        df_unicas_transformation
        .select(
            col("tracking_id").cast(StringType()),
            col("pedido_id").cast(StringType()),
            col("courier").cast(StringType()),
            col("estado_entrega").cast(StringType()),
            col("sucursal_origen").cast(DecimalType(12,2)),
            col("fecha_actualizacion").cast(TimestampType()),
            col("ingestion_at"),
            col("source_system"),
            col("updated_at")
        )
    )

@dp.materialized_view(
name=f"{catalog}.{schema_silver}.slv_quarantine",
comment="Cuarentena consolidada de registros rechazados"
)

def slv_quarantine():

    q_ventas = (
        spark.read.table(
            f"{catalog}.{schema_bronze}.brz_ventas_sucursales"
        )
        .filter(
            col("monto_total").cast("double").isNull()
            |
            (col("monto_total").cast("double") <= 0)
        )
        .select(
            lit("ventas").alias("source_table"),
            lit("monto_total invalido").alias("motivo_rechazo"),
            current_timestamp().alias("fecha_rechazo"),
            col("ingestion_at"),
            col("source_file"),
            to_json(struct("*")).alias("payload")
        )
    )

    q_devoluciones = (
        spark.read.table(
            f"{catalog}.{schema_bronze}.brz_devoluciones"
        )
        .filter(
            col("monto_reembolso").cast("double").isNull()
            |
            (col("monto_reembolso").cast("double") <= 0)
        )
        .select(
            lit("devoluciones").alias("source_table"),
            lit("monto_reembolso invalido").alias("motivo_rechazo"),
            current_timestamp().alias("fecha_rechazo"),
            col("ingestion_at"),
            col("source_file"),
            to_json(struct("*")).alias("payload")
        )
    )

    q_resenas = (
        spark.read.table(
            f"{catalog}.{schema_bronze}.brz_resenas"
        )
        .filter(
            col("calificacion").cast("int").isNull()
            |
            (~col("calificacion").cast("int").between(1, 5))
        )
        .select(
            lit("resenas").alias("source_table"),
            lit("calificacion fuera de rango").alias("motivo_rechazo"),
            current_timestamp().alias("fecha_rechazo"),
            col("ingestion_at"),
            col("source_file"),
            to_json(struct("*")).alias("payload")
        )
    )

    q_empleados = (
        spark.read.table(
            f"{catalog}.{schema_bronze}.brz_empleados_rrhh"
        )
        .filter(
            col("dni").isNull()
        )
        .select(
            lit("empleados").alias("source_table"),
            lit("dni nulo").alias("motivo_rechazo"),
            current_timestamp().alias("fecha_rechazo"),
            col("ingestion_at"),
            col("source_file"),
            to_json(struct("*")).alias("payload")
        )
    )

    q_productos = (
        spark.read.table(
            f"{catalog}.{schema_bronze}.brz_productos"
        )
        .filter(
            col("precio_lista").cast("double").isNull()
            |
            (col("precio_lista").cast("double") <= 0)
        )
        .select(
            lit("productos").alias("source_table"),
            lit("precio_lista invalido").alias("motivo_rechazo"),
            current_timestamp().alias("fecha_rechazo"),
            col("ingestion_at"),
            col("source_file"),
            to_json(struct("*")).alias("payload")
        )
    )

    q_tracking = (
        spark.read.table(
            f"{catalog}.{schema_bronze}.brz_tracking"
        )
        .filter(
            ~col("estado_entrega").isin(
                "En Camino",
                "En Transito",
                "Pendiente",
                "Entregado",
                "Devuelto"
            )
        )
        .select(
            lit("tracking").alias("source_table"),
            lit("estado_entrega invalido").alias("motivo_rechazo"),
            current_timestamp().alias("fecha_rechazo"),
            col("ingestion_at"),
            lit("Azure SQL Database").alias("source_file"),
            to_json(struct("*")).alias("payload")
        )
    )

    return (
        q_ventas
        .unionByName(q_devoluciones)
        .unionByName(q_resenas)
        .unionByName(q_empleados)
        .unionByName(q_productos)
        .unionByName(q_tracking)
    )
