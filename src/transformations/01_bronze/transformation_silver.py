from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, lit, to_date

@dp.table(
    name="dbelectrocasa.silver.slv_ventas"
)

@dp.expect_or_drop("monto_total_valido", "monto_total IS NOT NULL AND monto_total > 0")
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
    .withColumn("sucursal_id",coalesce(col("sucursal_id"),lit("SIN_SUCURSAL")))
  ).select(
    
  )

  
