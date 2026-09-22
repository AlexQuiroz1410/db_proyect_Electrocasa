from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, lit, to_date, current_timestamp, trim, initcap, when, regexp_replace
from pyspark.sql.types import StringType,IntegerType,DecimalType

@dp.table(
    name="dbelectrocasa.silver.slv_ventas"
)

@dp.expect_or_drop("monto_total_valido", "CAST(monto_total AS DOUBLE) IS NOT NULL AND CAST(monto_total AS DOUBLE) > 0")
@dp.expect_all({"sucursal_informada":"sucursal_id IS NOT NULL"})

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
        .withColumn("updated_at", current_timestamp())
    )
    return (
        df_unicas
        .select(
            col("venta_id").cast(StringType()),
            col("sucursal_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("cantidad").cast(IntegerType()),
            col("monto_total").cast(DecimalType()),
            col("metodo_pago").cast(StringType()),
            col("fecha_venta"),
            col("canal").cast(StringType()),
            col("updated_at")
        )
    )


@dp.table(
    name="dbelectrocasa.silver.slv_devoluciones"
)

@dp.expect_or_drop("monto_reembolso_valido", "CAST(monto_reembolso AS DOUBLE) IS NOT NULL AND CAST(monto_reembolso AS DOUBLE) > 0")
@dp.expect_all({"pedido_informado":"pedido_id IS NOT NULL"})

def slv_devoluciones():
    df_transformation = spark.read.table("dbelectrocasa.bronze.brz_devoluciones")
    df_unicas = (
        df_transformation.dropDuplicates(["devolucion_id"])
        .withColumn("motivo",initcap(trim(regexp_replace(col("motivo","_"," ")))))
        .withColumn("fecha_devolucion",to_date(col("fecha_devolucion")))
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
            col("monto_reembolso").cast(DecimalType()),
            col("fecha_devolucion"),
            col("updated_at")
        )
    )



  
