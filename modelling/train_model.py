import pickle
import polars as pl
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LogisticRegression
from pathlib import Path
import warnings, os

work_dir = str(Path(__file__).parents[0])

warnings.filterwarnings('ignore')

def preprocess_data(data) -> pl.DataFrame:
    df = pl.LazyFrame.from_json(data)
    df = df.with_columns(
        pl.when(pl.col('OfferStatus') == 'Hired')
        .then('1')
        .otherwise('0')
    )
    df = df.drop([
        'CandidateID', 'Name',
        'ApplicationDate', 'InterviewDate'
    ])
    return df

def prepOneHotEncoder(df, col, pathPackages) -> pd.DataFrame:
    oneHotEncoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
    dfOneHotEncoder = pd.DataFrame(
        oneHotEncoder.fit_transform(df[[col]]),
        columns=[col + "_" + str(i+1) for i in range(len(oneHotEncoder.categories_[0]))]
    )

    filename = 'prep' + col + '.pkl'
    dump_dir = os.path.join(pathPackages, filename)
    pickle.dump(oneHotEncoder, open(dump_dir, 'wb'))
    print(f"Preprocessing data {col} has been saved...")

    df = pd.concat([df.drop(col, axis=1), dfOneHotEncoder], axis=1)
    return df

def prepStandardScaler(df, col, pathPackages) -> pd.DataFrame:
    scaler = StandardScaler()
    df[col] = scaler.fit_transform(df[[col]])

    filename = 'prep' + col + '.pkl'
    dump_dir = os.path.join(pathPackages, filename)
    pickle.dump(scaler, open(dump_dir, 'wb'))
    print(f"Preprocessing data {col} has been saved...")

    return df

def train_model():
    data = pl.scan_csv('data/data_recruitment_selection_update.csv')
    model_dir = os.path.join(
        work_dir, 'model'
    )
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    categorical_cols = ['Gender', 'Position', 'Status']
    numerical_cols = ['Age']

    data = data.with_columns(
        pl.when(pl.col('OfferStatus') == 'Hired')
        .then(1)
        .otherwise(0).alias('OfferStatus')
    )
    data = data.drop(
        [
            'CandidateID', 'Name',
            'ApplicationDate', 'InterviewDate'
        ]
    )
    data = data.collect().to_pandas()
    for col in categorical_cols:
        data = prepOneHotEncoder(data, col, model_dir)

    for col in numerical_cols:
        data = prepStandardScaler(data, col, model_dir)

    X = data.drop(columns=['OfferStatus']).values
    y = data['OfferStatus'].values

    model = LogisticRegression()
    model.fit(X, y)

    dump_dir = os.path.join(model_dir, 'modelRecruitment.pkl')
    with open(dump_dir, 'wb') \
        as file:
        pickle.dump(model, file)

if __name__ == "__main__":
    train_model()
