import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import polars as pl
from jinja2 import Template

load_dotenv(override=True)

connection_string = os.getenv("DATABASE_URL")


def execute_query(query: str, params=None):
    conn = psycopg2.connect(connection_string)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return [dict(row) for row in cursor.fetchall()]
            else:
                conn.commit()
                return cursor.rowcount
    finally:
        conn.close()


def execute_sql_file(file_path: str, params=None):
    with open(file_path, "r") as f:
        sql_content = f.read()

    if params:
        template = Template(sql_content)
        sql_content = template.render(**params)

    return execute_query(sql_content)


def write_dataframe(df: pl.DataFrame, table_name: str):
    df.write_database(
        table_name=table_name,
        connection=connection_string,
        if_table_exists="replace",
        engine="sqlalchemy",
    )


def read_dataframe(table_name: str) -> pl.DataFrame:
    return pl.read_database_uri(
        query=f"SELECT * FROM {table_name}",
        uri=connection_string,
    )
