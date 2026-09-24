from pyspark import pipelines as dp
from pyspark.sql.functions import col,count,countDistinct,sum,avg,round,year,month,when,coalesce,lit

catalog = spark.conf.get("bundle.catalog")
schema_silver = spark.conf.get("bundle.schema_silver")
schema_gold = spark.conf.get("bundle.schema_gold")


# ============================================================
# 1. VENTAS Y TICKET PROMEDIO POR SUCURSAL Y MES
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.gld_ventas_sucursal_mes",
    comment="Ventas y ticket promedio por sucursal y mes"
)
def gld_ventas_sucursal_mes():

    df_ventas = spark.read.table(f"{catalog}.{schema_silver}.slv_ventas")

    return (
        df_ventas
        .groupBy(
            col("sucursal_id"),
            year("fecha_venta").alias("anio"),
            month("fecha_venta").alias("mes")
        )
        .agg(
            countDistinct("venta_id").alias("cantidad_ventas"),
            sum("cantidad").alias("unidades_vendidas"),
            round(
                sum("monto_total"),
                2
            ).alias("ventas_totales"),
            round(
                avg("monto_total"),
                2
            ).alias("ticket_promedio")
        )
    )


# ============================================================
# 2. RANKING PRODUCTOS:
#    MÁS VENDIDOS Y CON MÁS DEVOLUCIONES
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.gld_ranking_productos",
    comment="Ranking de productos por ventas y devoluciones"
)
def gld_ranking_productos():

    df_ventas = spark.read.table(f"{catalog}.{schema_silver}.slv_ventas")

    df_devoluciones = spark.read.table(f"{catalog}.{schema_silver}.slv_devoluciones")

    df_productos = spark.read.table(f"{catalog}.{schema_silver}.slv_productos")


    # Ventas agregadas primero por producto
    ventas_producto = (
        df_ventas
        .groupBy("producto_id")
        .agg(
            sum("cantidad").alias("unidades_vendidas"),
            countDistinct("venta_id").alias("cantidad_ventas"),
            round(
                sum("monto_total"),
                2
            ).alias("monto_vendido")
        )
    )


    # Devoluciones agregadas primero por producto
    devoluciones_producto = (
        df_devoluciones
        .groupBy("producto_id")
        .agg(
            countDistinct("devolucion_id")
            .alias("cantidad_devoluciones"),

            round(
                sum("monto_reembolso"),
                2
            ).alias("monto_reembolsado")
        )
    )


    return (
        df_productos.alias("p")

        .join(
            ventas_producto.alias("v"),
            col("p.producto_id") == col("v.producto_id"),
            "left"
        )
        .join(
            devoluciones_producto.alias("d"),
            col("p.producto_id") == col("d.producto_id"),
            "left"
        )
        .select(
            col("p.producto_id"),
            col("p.nombre_producto"),
            col("p.categoria"),
            col("p.marca"),

            coalesce(
                col("v.unidades_vendidas"),
                lit(0)
            ).alias("unidades_vendidas"),

            coalesce(
                col("v.cantidad_ventas"),
                lit(0)
            ).alias("cantidad_ventas"),

            coalesce(
                col("v.monto_vendido"),
                lit(0)
            ).alias("monto_vendido"),

            coalesce(
                col("d.cantidad_devoluciones"),
                lit(0)
            ).alias("cantidad_devoluciones"),

            coalesce(
                col("d.monto_reembolsado"),
                lit(0)
            ).alias("monto_reembolsado")
        )
    )


# ============================================================
# 3. DOTACIÓN ACTIVA DE PERSONAL POR SUCURSAL
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.gld_dotacion_sucursal",
    comment="Dotación activa de empleados por sucursal"
)
def gld_dotacion_sucursal():

    df_empleados = spark.read.table(f"{catalog}.{schema_silver}.slv_empleados_actual")

    return (
        df_empleados
        .groupBy("sucursal_id")
        .agg(
            countDistinct("id_empleado")
            .alias("empleados_activos")
        )
    )


# ============================================================
# 4. TASA DE RESEÑAS NEGATIVAS POR CATEGORÍA
# ============================================================

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.gld_resenas_categoria",
    comment="Tasa de reseñas negativas por categoría de producto"
)
def gld_resenas_categoria():

    df_resenas = spark.read.table(f"{catalog}.{schema_silver}.slv_resenas")

    df_productos = spark.read.table(f"{catalog}.{schema_silver}.slv_productos")


    df_base = (
        df_resenas.alias("r")
        .join(
            df_productos.alias("p"),
            col("r.producto_id") == col("p.producto_id"),
            "inner"
        )
        .select(
            col("r.resena_id"),
            col("r.calificacion"),
            col("p.categoria")
        )
    )


    return (
        df_base
        .groupBy("categoria")
        .agg(
            countDistinct("resena_id")
            .alias("total_resenas"),

            countDistinct(
                when(
                    col("calificacion") <= 2,
                    col("resena_id")
                )
            ).alias("resenas_negativas"),

            round(
                avg("calificacion"),
                2
            ).alias("calificacion_promedio")
        )

        .withColumn(
            "tasa_resenas_negativas",
            round(
                (
                    col("resenas_negativas")
                    / col("total_resenas")
                ) * 100,
                2
            )
        )
    )