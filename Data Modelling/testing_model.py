"""
bikin model pakae parameter yang udah diuji
training : semua data jakarta selatan
testing : dibagi ke setiap kecamatan di jakarta utara
ada 3 experiment:
- 0% data testing
- 5% data testing
- 10 % data testing or add one kecamatan to the training dataset
"""

import pandas as pd
import xgboost as xgb
import json
import os
import glob
import logging
from sklearn import metrics
import traceback

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

iterasi = 9
save_model = True
export_geom = True
dropped_columns = [
    "s_av_closeness_global_street_50",
    "s_max_closeness_global_street_50",
    "s_av_straightness_global_street_50",
    "s_max_straightness_global_street_50",
    "s_av_meshedness_global_street_50",
    "s_max_meshedness_global_street_50",
    "s_av_closeness_global_street_150",
    "s_max_closeness_global_street_150",
    "s_av_straightness_global_street_150",
    "s_max_straightness_global_street_150",
    "s_av_meshedness_global_street_150",
    "s_max_meshedness_global_street_150",
    "s_av_closeness_global_street_300",
    "s_max_closeness_global_street_300",
    "s_av_straightness_global_street_300",
    "s_max_straightness_global_street_300",
    "s_av_meshedness_global_street_300",
    "s_max_meshedness_global_street_300",
    "nodeID",
]

with open("config.json", "r") as config_file:
    config = json.load(config_file)

folder_training_in = config.get("testing_model")["input_training"]
folder_testing_in = config.get("testing_model")["input_testing"]
report_folder = config.get("testing_model")["report"]
results_folder = config.get("testing_model")["results"]

os.makedirs(report_folder, exist_ok=True)
os.makedirs(results_folder, exist_ok=True)

experiment = f"Exp_{iterasi}.csv"

training_files = glob.glob(os.path.join(folder_training_in, "*.csv"))
testing_files = glob.glob(os.path.join(folder_testing_in, "Kelapa Gading.csv"))

# call every training, and then merged it. the merged one is corrupt

training_dfs = []
logging.info("Merging training dataset...")
for training_file in training_files:
    df = pd.read_csv(training_file)

    try:
        df = df.drop(columns=dropped_columns, axis=1)
    except:
        pass

    training_dfs.append(df)

training_df = pd.concat(training_dfs, axis=0, ignore_index=True)

logging.info("Prepare training dataset...")

X_train = training_df.drop(
    columns=["bID_kec", "label", "geometry"], axis=1
).copy()
y_train = training_df[["label"]].copy()
print(len(X_train.columns))
dtrain = xgb.DMatrix(data=X_train, label=y_train)

params = {
    # Need to check the ideal eta and estimators before run this
    "objective": "binary:logistic",
    "eta": 0.05,
    "max_depth": 5,
    "colsample_bytree": 0.7,
    "min_child_weight": 5,
    "subsample": 0.95,
    "scale_pos_weight": 0.4,
    "gamma": 0.1,
    "random_state": 10,
    "reg_alpha": 1.5,
    "reg_lambda": 0.1,
}

logging.info("Training model...")
clf_xgb = xgb.train(
    params,
    dtrain,
    num_boost_round=350,  # need to be defined later
    verbose_eval=False,
)

if save_model == True:
    model_path = "Data Modelling\\Models"
    os.makedirs(model_path, exist_ok=True)
    clf_xgb.save_model(
        os.path.join(model_path, f"model_noleak_{iterasi}.json")
    )

test_datas = []
for testing_file in testing_files:
    try:
        name = os.path.basename(testing_file)
        logging.info(f"{name}: Predicting... ")

        test_df = pd.read_csv(testing_file)
        if name == "Koja.csv":
            test_df["building_residential"] = 0
        test_df = test_df[training_df.columns]

        X_test = test_df.drop(
            columns=["bID_kec", "label", "geometry"], axis=1
        ).copy()
        y_test = test_df[["label"]].copy()

        dtest = xgb.DMatrix(data=X_test, label=y_test)

        y_pred = clf_xgb.predict(dtest)

        y_pred_labels = (y_pred >= 0.518).astype(int)

        test_df["pred_proba"] = y_pred
        test_df["pred_labels"] = y_pred_labels
        test_df["prob_loss"] = abs(test_df["pred_proba"] - test_df["label"])
        test_df = test_df[
            ["bID_kec", "label", "pred_labels", "pred_proba", "geometry"]
        ]
        (
            test_df.to_csv(
                os.path.join(
                    results_folder,
                    name.replace(".csv", f"_noleak_{iterasi}.csv"),
                ),
                index=False,
            )
            if export_geom == True
            else None
        )

        accuracy = metrics.accuracy_score(y_test, y_pred_labels)
        bal_accuracy = metrics.balanced_accuracy_score(y_test, y_pred_labels)
        f1_score_pos = metrics.f1_score(y_test, y_pred_labels)
        f1_score_neg = metrics.f1_score(y_test, y_pred_labels, pos_label=0)
        f1_score_ave = (f1_score_pos + f1_score_neg) / 2
        recall = metrics.recall_score(y_test, y_pred_labels)
        specificity = metrics.recall_score(y_test, y_pred_labels, pos_label=0)
        roc_auc = metrics.roc_auc_score(y_test, y_pred_labels)
        logloss = metrics.log_loss(y_test, y_pred)

        data = [
            name,
            accuracy,
            bal_accuracy,
            f1_score_pos,
            f1_score_neg,
            f1_score_ave,
            recall,
            specificity,
            roc_auc,
            logloss,
        ]

        test_datas.append(data)
    except Exception as e:
        logging.error(f"{name}: {e}")
        traceback.print_exc()
        continue

logging.info("Create report...")
data_df = pd.DataFrame(
    test_datas,
    columns=[
        "kecamatan",
        "accuracy",
        "bal_accuracy",
        "f1_score_pos",
        "f1_score_neg",
        "f1_score_ave",
        "recall",
        "specificity",
        "roc_auc",
        "logloss",
    ],
)

data_df.to_csv(os.path.join(report_folder, experiment), index=False)
