import polars as pl
import polars.selectors as cs
import sys, os
from pathlib import Path
import psycopg2

data_path = str(Path(__file__).parents[0])
sys.path.append(data_path)

from dump_data.dump import insert_postgres

class DumpWarehouse:
    @staticmethod
    def connector_postgres(query):
        uri_postgres = "postgresql://user:userpassword@localhost:5433/final_project"
        df = pl.read_database_uri(
            query=query, uri=uri_postgres, engine="adbc"
        )
        return pl.LazyFrame(df)

    @staticmethod
    def connector_mysql(query):
        uri_mysql = "mysql://root:rootpassword@localhost:3306/final_project"
        df = pl.read_database_uri(
            query=query, uri=uri_mysql, engine="connectorx"
        ).cast({cs.exclude('EmployeeID') : pl.String})
        return pl.LazyFrame(df)

    @staticmethod
    def dump_postgres(df : pl.LazyFrame, table_name: str):
        insert_postgres(
            df = df.collect(),
            table_name = table_name,
            warehouse = True
        )

if __name__ == "__main__":
    from modelling.predict import Predict
    dw = DumpWarehouse()

    query_payroll = "SELECT * FROM management_payroll"
    df_payroll = dw.connector_postgres(query_payroll)

    query_management = "SELECT * FROM performance_management"
    df_management = dw.connector_postgres(query_management)

    query_training = "SELECT * FROM training_development"
    df_training = dw.connector_mysql(query_training)

    df_prediction = Predict().predict()

    payroll_column = [
        'PayrollID', 'CandidateID', 'EmployeeID',
        'PaymentDate', 'Salary', 'OvertimePay',
        'TrainingID', 'StartTraining', 'FinishTraining',
        'StatusTraining'
    ]

    fact_payroll = (
        (
            df_payroll
            .with_row_index(offset=1)
            .rename({'index' : 'PayrollID'})
            .cast({'PayrollID' : pl.Int32})
        )
        .join(
            df_prediction, on = ['Name', 'Position', 'Gender'], how = 'left'
        )
        .unique(subset = 'EmployeeID', keep='first')
        .join(
            (
                df_training.join(
                    (
                        df_training
                        .select(['TrainingProgram'])
                        .unique(keep = 'first')
                        .with_row_index(offset=1)
                        .rename({'index' : 'TrainingID'})
                        .cast({'TrainingID' : pl.Int32})
                    ),
                    on = 'TrainingProgram', how = 'left'
                )
                .rename(
                    {
                        'StartDate' : 'StartTraining',
                        'EndDate' : 'FinishTraining'
                    }
                )
            ),
            on = 'EmployeeID', how = 'left',
            suffix = 'Training'
        )
        .select(payroll_column)
        .sort('PayrollID')
        .cast(
            {
                'StartTraining' : pl.Date,
                'FinishTraining' : pl.Date
            }
        )
    )

    employee_column = [
        'EmployeeID', 'Name', 'Gender',
        'Age', 'Department', 'Position',
        'ReviewPeriod', 'Rating', 'Comments'
    ]

    dim_employeed = (
        df_management
        .join(
            df_payroll, on = 'EmployeeID', how = 'left'
        )
        .select(employee_column)
    )

    training_column = [
        'TrainingID', 'TrainingProgram'
    ]

    dim_training = (
        df_training
        .select(['TrainingProgram'])
        .unique()
        .with_row_index(offset=1)
        .rename({'index' : 'TrainingID'})
        .cast({'TrainingID' : pl.Int32})
    )

    recruitment_column = [
        'CandidateID', 'Name',
        'Gender', 'Age', 'Position',
        'ApplicationDate', 'Status',
        'InterviewDate', 'OfferStatus',
        'Prediction'
    ]

    dim_recruitment = (
        df_prediction
        .with_columns(
            pl.when(pl.col('Prediction') == 1)
            .then(pl.lit('Hired'))
            .otherwise(pl.lit('Rejected'))
            .alias('Prediction')
        )
        .select(recruitment_column)
        .cast(
            {
                'ApplicationDate' : pl.Date,
                'InterviewDate' : pl.Date
            }
        )
    )

    dw.dump_postgres(fact_payroll, 'payroll')
    dw.dump_postgres(dim_employeed, 'employee')
    dw.dump_postgres(dim_training, 'training')
    dw.dump_postgres(dim_recruitment, 'recruitment')
