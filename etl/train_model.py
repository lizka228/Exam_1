import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import pickle

import logging

def train_model(**kwargs):
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(key="preprocessed_data", task_ids="preprocessing_data")
    df = pd.read_json(df_json)
    
    X = df.iloc[:, 1:]
    y = df.iloc[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    if X_train.shape[0] < 10:
        logging.error("Not enoudh data for training! STOP DAG.")
        raise AirflowException("The model cannot be trained on a small dataset!") 

    model = LogisticRegression()
    model.fit(X_train, y_train)
    
    ti.xcom_push(key="model", value=pickle.dumps(model).hex())
    ti.xcom_push(key="X_test", value=X_test.to_json())
    ti.xcom_push(key="y_test", value=y_test.to_json())
    
    logging.info("Training model is success!!!")