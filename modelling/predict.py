import pymongo
import pickle
import pandas as pd
import polars as pl
from pathlib import Path
import warnings, os

warnings.filterwarnings('ignore')

class Predict:
    def __init__(self):
        self.collection = self.connect_mongo()
        self.dir = os.path.join(
            str(Path(__file__).parents[0]), 'model'
        )

    def connect_mongo(self):
        client = pymongo.MongoClient("mongodb://root:rootpassword@localhost:27018/")
        db = client["ftde02"]
        collection = db["final-project"]
        return collection

    def preprocess_data(self) -> pl.LazyFrame:
        # df = self.collection.find()
        # df = pl.LazyFrame(df)
        # temp code
        df = pl.scan_csv(
            os.path.join(str(Path(__file__).parents[1]), 'data', 'data_recruitment_selection_update.csv')
        )

        return df

    def prepOneHotEncoder(self, df, col):
        load_dir = os.path.join(
            self.dir, 'prep' + col + '.pkl'
        )
        oneHotEncoder = pickle.load(open(load_dir, 'rb'))
        dfOneHotEncoder = pd.DataFrame(
            oneHotEncoder.transform(df[[col]]),
            columns=[col + "_" + str(i+1) for i in range(len(oneHotEncoder.categories_[0]))]
        )
        df = pd.concat([df.drop(col, axis=1), dfOneHotEncoder], axis=1)
        return df

    def prepStandardScaler(self, df, col):
        load_dir = os.path.join(
            self.dir, 'prep' + col + '.pkl'
        )
        scaler = pickle.load(open(load_dir, 'rb'))
        df[col] = scaler.transform(df[[col]])
        return df

    def predict(self) -> pl.LazyFrame:
        data = self.preprocess_data()
        df = data.collect().to_pandas()
        categorical_cols = ['Gender', 'Position', 'Status']
        numerical_cols = ['Age']

        list_cols = df.columns.to_list()
        exclude_cols = list(
            set(list_cols) - set(categorical_cols + numerical_cols)
        )
        df = df.drop(exclude_cols, axis=1)
        for col in categorical_cols:
            df = self.prepOneHotEncoder(df, col)

        for col in numerical_cols:
            df = self.prepStandardScaler(df, col)

        model_dir = os.path.join(
            self.dir, 'modelRecruitment.pkl'
        )
        X = df.values
        model = pickle.load(open(model_dir, 'rb'))
        y = model.predict(X)
        data = pl.concat(
            [
                data,
                pl.LazyFrame({
                    'Prediction': y
                })
            ], how = 'horizontal'
        )
        return data

if __name__ == "__main__":
    Predict().predict()