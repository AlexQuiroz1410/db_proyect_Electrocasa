def get_sql_credentials():

    return {
        "user": dbutils.secrets.get(
            scope="electrocasa",
            key="sql-user"
        ),
        "password": dbutils.secrets.get(
            scope="electrocasa",
            key="sql-password"
        )
    }