from pyspark.sql.types import StructType, StructField, StringType

def schema_ventas_sucursal():

    schema = StructType([
        StructField("venta_id", StringType(), True),
        StructField("sucursal_id", StringType(), True),
        StructField("producto_id", StringType(), True),
        StructField("cantidad", StringType(), True),
        StructField("monto_total", StringType(), True),
        StructField("metodo_pago", StringType(), True),
        StructField("fecha_venta", StringType(), True),
        StructField("canal", StringType(), True)
    ])

    return schema