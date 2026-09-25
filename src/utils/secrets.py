SCOPE = "electrocasa-secrets"

def get_sql_credentials():
    return {
        "user": dbutils.secrets.get(
            scope=SCOPE,
            key="sql-user"
        ),
        "password": dbutils.secrets.get(
            scope=SCOPE,
            key="sql-password"
        )
    }