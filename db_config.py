# db_config.py
import sqlalchemy

def get_engine():
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc://localhost/cargo_pipeline"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )
    return engine