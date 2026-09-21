from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

def schema_resena():

    schema_respuestas = StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ])
    
    df_reader = StructType([
        StructField("resena_id",   StringType(), True),
        StructField("producto_id", StringType(), True),
        StructField("cliente_id",  StringType(), True),
        StructField("calificacion", StringType(), True),
        StructField("comentario",  StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("respuestas",  ArrayType(schema_respuestas), True),
        StructField("fecha_resena", StringType(), True),
    ])

    return df_reader