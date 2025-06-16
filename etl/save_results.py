import json
import pickle
import logging
import os
from dotenv import load_dotenv
import yadisk
import datetime

load_dotenv()
MODEL_PATH = os.getenv("MODEL_PATH")
METRICS_PATH = os.getenv("METRICS_PATH")
YADISK_TOKEN = os.getenv("YADISK_TOKEN")
PAPKA = os.getenv("PAPKA")

def save_results(**kwargs):
    ti = kwargs["ti"]
    model_pickle = ti.xcom_pull(key="model", task_ids="train_model")
    metrics_json = ti.xcom_pull(key="metrics", task_ids="evaluate_model")

    model = pickle.loads(bytes.fromhex(model_pickle))
    metrics = json.loads(metrics_json)
    try:
      with open(MODEL_PATH, "wb") as f:
          pickle.dump(model, f)
      logging.info("Модель сохранена в папку /results.")
    except:
        logging.warning("Не смогли сохранить, так как не указан путь для сохранения модели!!!")
        
    try:
      with open(METRICS_PATH, "w") as f:
          json.dump(metrics, f, indent=4)
      logging.info("Метрики сохранены в папку /results.")
    except:
        logging.warning("Не смогли сохранить, так как не указан путь для сохранения метрик!!!")

    
    try:
        client = yadisk.Client(token=YADISK_TOKEN)
        with client:
            if not client.exists(f"/{PAPKA}/"):
              client.mkdir(f"/{PAPKA}/")  
              
            current_date = datetime.date.today().isoformat()
            
            client.upload(MODEL_PATH, f"/{PAPKA}/model_{current_date}.pkl", overwrite=True)
            logging.info(f"Модель загружена на Яндекс Диск: /{PAPKA}/model_{current_date}.pkl")
            client.upload(METRICS_PATH, f"/{PAPKA}/model_quality_{current_date}.json", overwrite=True)
            logging.info(f"Метрики загружены на Яндекс Диск: /{PAPKA}/model_quality_{current_date}.json")
    except Exception as e:
        logging.warning("Не смогли сохранить на Яндекс диск, так как не указан токен и (или) пути к файлам!!!", e)

