import pandas as pd
import logging
import os
from dotenv import load_dotenv

load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")
    
def load_data(file_path: str = DATA_PATH, **kwargs):
    ti = kwargs["ti"]
    try:
      df = pd.read_csv(file_path)
      ti.xcom_push(key="data", value=df.to_json())
      logging.info(f"Данные загружены и отправлены в XCom, их размерность: {df.shape}")
    except:
      logging.error("Ошибка загрузки данных!")
