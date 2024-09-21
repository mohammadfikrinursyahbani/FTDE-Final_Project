import polars as pl
import sys
from pathlib import Path

data_path = str(Path(__file__).resolve().parents[1])

sys.path.append(data_path)

def insert_postgres(df: pl.DataFrame,table_name: str, warehouse = True):
    if warehouse:
        connection = "postgresql://fikri:fikri@dev-fikri.my.id:5432/final_project"
    else:
        connection = "postgresql://user:userpassword@localhost:5433/final_project"

    df.write_database(
        table_name = table_name,
        connection= connection,
        engine="adbc"
    )

def insert_mysql(df: pl.DataFrame, table_name: str):
    df.write_database(
        table_name = table_name,
        connection="mysql+mysqlconnector://root:rootpassword@localhost:3306/final_project"
    )

if __name__ == "__main__":
    df_management_payroll = pl.read_csv("data/data_management_payroll_update.csv")
    df_performance_management = pl.read_csv("data/data_performance_management_update.csv")
    df_training_development = pl.read_csv("data/data_training_development_update.csv")

    insert_postgres(df_management_payroll, "management_payroll", False)
    insert_postgres(df_performance_management, "performance_management", False)
    insert_mysql(df_training_development, "training_development")
