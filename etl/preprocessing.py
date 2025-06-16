import pandas as pd
from sklearn.preprocessing import StandardScaler
import logging

def preprocess(**kwargs):
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(key="data", task_ids="load_data")
    df = pd.read_json(df_json)
    
    df_new = df.iloc[:, 1:] # дропнули идентификаторы и последний непонятный столбец
    if df_new.isnull().sum().sum() > 0:
        logging.warning("NaN values! Drop NaN.")
        df_new = df_new.dropna(axis=0)

    scaler = StandardScaler()
    df_new.iloc[:, 1:] = scaler.fit_transform(df_new.iloc[:, 1:])
    df_new.iloc[:,0] = df_new.iloc[:,0].apply(lambda x: 1 if x == 'M' else 0) # злокачественная опухоль - 1, доброкачественная - 0

    ti.xcom_push(key="preprocessed_data", value=df_new.to_json())
    logging.info(f"Данные предобработаны и отправлены в XCom, их размерность: {df_new.shape}")