SCOPE_NAME = "electrocasa-secrets"

def get_sql_credentials(dbutils):
    return {
    "user": dbutils.secrets.get(
        scope=SCOPE_NAME,
        key="sql-user"
    ),
    "password": dbutils.secrets.get(
        scope=SCOPE_NAME,
        key="sql-password"
    )
}