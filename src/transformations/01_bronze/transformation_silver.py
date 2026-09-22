from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, lit, to_date, current_timestamp, trim, initcap, when, regexp_replace, posexplode
from pyspark.sql.types import StringType,IntegerType,DecimalType, DateType, TimestampType

@dp.table(
    name="dbelectrocasa.silver.slv_ventas"
)

@dp.expect_or_drop("monto_total_valido", "monto_total IS NOT NULL AND monto_total > 0")

def slv_ventas():
    df_transformation = spark.read.table("dbelectrocasa.bronze.brz_ventas_sucursales")
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
            col("updated_at")
        )
    )


@dp.table(
    name="dbelectrocasa.silver.slv_devoluciones"
)

@dp.expect_or_drop("monto_reembolso_valido", """CAST(monto_reembolso AS DOUBLE) IS NOT NULL AND CAST(monto_reembolso AS DOUBLE) > 0""")

def slv_devoluciones():
    df_transformation = spark.read.table("dbelectrocasa.bronze.brz_devoluciones")
    df_unicas = (
        df_transformation.dropDuplicates(["devolucion_id"])
        .withColumn("motivo",initcap(trim(regexp_replace(col("motivo"),"_"," ")))))
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
            col("updated_at")
        )
    )

@dp.table(
    name="dbelectrocasa.silver.slv_resenas"
)

@dp.expect_or_drop("calificacion_valido","calificacion BETWEEN 1 AND 5 AND calificacion IS NOT NULL""")
@dp.expect_all({"fecha_resena_informado":"fecha_resena IS NOT NULL"})

def slv_resenas():
    df_transformation = spark.read.table("dbelectrocasa.bronze.brz_resenas")
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
            col("updated_at")
        )
    )


@dp.table(
    name="dbelectrocasa.silver.slv_resenas_detalle"
)

def slv_resenas_detalle():
    df_transformation = spark.read.table("dbelectrocasa.bronze.brz_resenas")
    df_unicas = (
        df_transformation
        .dropDuplicates(["resena_id"])
        .fillna(
            {
                "comentario":"Sin Comentario"
            }
        )
    )

    df_respuestas_posexplode = df_unicas.select(
        "resena_id",
        posexplode("respuestas").alias("pos", "respuesta"),"fecha_resena"
    ).select(
        "resena_id",
        col("pos").alias("pos_respuesta"),
        col("respuesta.autor").alias("autor"),
        col("respuesta.texto").alias("texto")
    )
    
    return (
        df_respuestas_posexplode
        .select(
            col("resena_id").cast(StringType()),
            col("pos_respuesta").cast(IntegerType()),
            col("autor").cast(StringType()),
            col("texto").cast(StringType())
        )
    )


@dp.table(
    name="dbelectrocasa.silver.slv_productos"
)

@dp.expect_or_drop("precio_valido","""CAST(precio_lista AS DOUBLE) IS NOT NULL AND CAST(precio_lista AS DOUBLE) > 0""")

def slv_productos():
    df_transformation = spark.read.table("dbelectrocasa.bronze.brz_productos")
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
            col("updated_at")
        )
    )

@dp.temporary_view(
    name="view_empleados",
    comment="Vista limpia de empleados"
)

@dp.expect_or_drop("dni_valido","dni IS NOT NULL")
@dp.expect_all(
    {
    "fecha_evento_validacion":"fecha_evento IS NOT NULL"
    }
)

def staging_empleados():
    df_transformation = sparkStream.read.table("dbelectrocasa.bronze.brz_empleados_rrhh")
    df_limpio = df_transformation.dropna(subset=["dni"])
    df_estandarizado = (
        df_limpio
        .withColumn("tipo_evento",initcap(trim(col("tipo_evento"))))
        .withColumn(
            "tipo_evento",
            when(col("tipo_evento") == "Cambio_Salario","Cambio Salario")
            .otherwise(col("tipo_evento"))
        )
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
            col("updated_at"),
        )
    )
    return df_estandarizado

dp.create_streaming_table(
    name="dbelectrocasa.silver.slv_empleados_hist",
    comment="Lista de Empleados"
)

dp.create_auto_cdc_flow(
    source= "view_empleados",
    target= "dbelectrocasa.silver.slv_empleados_hist",
    keys=["id_empleado"],
    sequence_by="fecha_evento",
    column_list = ["nombre","dni","email","salario","sucursal_id","cargo","tipo_evento"],
    stored_as_scd_type="2",
    name="empleados_cdc_type2"
)

@dp.table(
    name="dbelectrocasa.silver.slv_empleados_actual"
)    
def slv_empleados_actual():

    df_actual = (spark.read.table("dbelectrocasa.silver.slv_empleados_hist")
        .filter(
            col("__END_AT").isNull()
        )
    )
    
    return df_actual
