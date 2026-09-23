# Databricks notebook source
# Creamos el Catalog
spark.sql(f"""CREATE CATALOG IF NOT EXISTS dbelectrocasa""")

# Creamos los schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS dbelectrocasa.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbelectrocasa.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS dbelectrocasa.gold")

# Creamos el volumen
spark.sql("CREATE VOLUME IF NOT EXISTS dbelectrocasa.default.vol_landing")

# COMMAND ----------

#Verificamos el entorno
display(spark.sql("""SHOW SCHEMAS IN dbelectrocasa"""))
display(spark.sql("""SELECT current_user() AS usuario_actual"""))

# Asumimos que se crearon los grupos a nivel de cuenta y generamos los permisos - sustento en Readme
spark.sql("""GRANT USE CATALOG ON CATALOG dbelectrocasa TO `electrocasa_ingenieria`""")
spark.sql("""GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dbelectrocasa.bronze TO `electrocasa_ingenieria`""")
spark.sql("""GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dbelectrocasa.silver TO `electrocasa_ingenieria`""")
spark.sql("""GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA dbelectrocasa.gold TO `electrocasa_ingenieria`""")

spark.sql("""GRANT USE CATALOG ON CATALOG dbelectrocasa TO `electrocasa_analistas`""")
spark.sql("""GRANT USE SCHEMA, SELECT ON SCHEMA dbelectrocasa.gold TO `electrocasa_analistas`""")

spark.sql("""GRANT USE CATALOG ON CATALOG dbelectrocasa TO `electrocasa_auditoria`""")
spark.sql("""GRANT USE SCHEMA, SELECT ON SCHEMA dbelectrocasa.gold TO `electrocasa_auditoria`""")

# COMMAND ----------

# Simulación USO REVOKE
spark.sql("""GRANT USE CATALOG ON CATALOG dbelectrocasa TO `electrocasa_analistas`""")
spark.sql("""GRANT USE SCHEMA, SELECT ON SCHEMA dbelectrocasa.default TO `electrocasa_analistas`""")

spark.sql("""REVOKE USE SCHEMA, SELECT ON SCHEMA dbelectrocasa.default FROM `electrocasa_analistas`""")
