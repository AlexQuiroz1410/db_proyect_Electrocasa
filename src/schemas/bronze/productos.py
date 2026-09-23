from pyspark.sql.types import StructType, StructField, StringType

def schema_productos():

    schema_producto = StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True)
    ])
    
    df_reader = StructType([
        StructField("producto_id",   StringType(), True),
        StructField("nombre_producto", StringType(), True),
        StructField("categoria",  StringType(), True),
        StructField("marca", StringType(), True),
        StructField("precio_lista",  StringType(), True)
    ])

    return df_reader