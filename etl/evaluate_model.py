from sklearn.metrics import accuracy_score,precision_score,recall_score,f1_score
import pickle
import json
import pandas as pd
import logging
    
def evaluate_model(**kwargs):
    ti = kwargs["ti"]
    
    model_pickle = ti.xcom_pull(key="model", task_ids="train_model")
    X_test_json = ti.xcom_pull(key="X_test", task_ids="train_model")
    y_test_json = ti.xcom_pull(key="y_test", task_ids="train_model")
    
    model = pickle.loads(bytes.fromhex(model_pickle))
    X_test = pd.read_json(X_test_json)
    y_test_dict = json.loads(y_test_json)
    y_test = pd.DataFrame(list(y_test_dict.values()))
    y_pred = model.predict(X_test)

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
    }
    
    ti.xcom_push(key="metrics", value=json.dumps(metrics))
    logging.info("Evaluate model is success!!!")