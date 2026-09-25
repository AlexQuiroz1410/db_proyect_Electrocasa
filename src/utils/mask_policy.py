#Crear tag
spark.sql("CREATE GOVERNED TAG IF NOT EXISTS pii DESCRIPTION 'Tipo de dato personal identificable que contiene la columna' VALUES ('dni', 'email', 'salario')")

#Relacionar con la columna correspondiente
spark.sql("ALTER TABLE dbelectrocasa.silver.slv_empleados_actual ALTER COLUMN dni SET TAGS ('pii' = 'dni')")
spark.sql("ALTER TABLE dbelectrocasa.silver.slv_empleados_actual ALTER COLUMN salario SET TAGS ('pii' = 'salario')")
spark.sql("ALTER TABLE dbelectrocasa.silver.slv_empleados_actual ALTER COLUMN dni SET TAGS ('pii' = 'dni')")

spark.sql("""
CREATE OR REPLACE FUNCTION dbelectrocasa.silver.mask_pii_generico(valor STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('electrocasa_ingenieria') THEN valor
    ELSE '***REDACTADO***'
END
""")