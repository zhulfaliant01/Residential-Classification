import pandas as pd
import xgboost as xgb
import json
import os
import glob
import logging
from sklearn import metrics
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

folder_in = config.get("initial_model")["input_5cv"]
folder_out = config.get("initial_model")["output"]
folder_report = config.get("initial_model")["report"]

os.makedirs(folder_out, exist_ok=True)
os.makedirs(folder_report, exist_ok=True)

report_file = r"Data Modelling/Iteration_resuls_6.csv"
results_columns = [
    "best_iteration",
    "logloss",
    "auc",
    "accuracy",
    "bal_accuracy",
    "f1_score_pos",
    "f1_score_neg",
    "f1_score_ave",
    "recall_pos",
    "recall_neg",
    "precision_pos",
    "precision_neg",
    "params",
]

if not os.path.exists(report_file):
    results_df = pd.DataFrame(columns=results_columns)
    results_df.to_csv(report_file, index=False)

training_files = glob.glob(os.path.join(folder_in, "*_train.csv"))
validation_files = glob.glob(os.path.join(folder_in, "*val.csv"))

report_list = []

ver = 23
export_geom = True


for train, val in zip(training_files, validation_files):
    cv = os.path.basename(train).replace("_train.csv", "")
    logging.info(f"{cv}: Start...")

    train_df = pd.read_csv(train)
    val_df = pd.read_csv(val)

    try:
        train_df = train_df.drop(columns=dropped_columns, axis=1)
    except:
        val_df = val_df.drop(columns=dropped_columns, axis=1)

    val_df = val_df[train_df.columns]  # to match the training columns

    X_train = train_df.drop(columns=["bID_kec", "label", "geometry"], axis=1).copy()
    y_train = train_df[["label"]].copy()

    X_val = val_df.drop(columns=["bID_kec", "label", "geometry"], axis=1).copy()
    y_val = val_df[["label"]].copy()

    logging.info(f"{cv}: Start DMatrix...")
    # Creating DMatrix for training and validation sets
    dtrain = xgb.DMatrix(data=X_train, label=y_train)
    dval = xgb.DMatrix(data=X_val, label=y_val)

    logging.info(f"{cv}: Start training...")

    eval_metric = "auc"

    params = {
        "objective": "binary:logistic",
        "eval_metric": eval_metric,
        # "max_delta_step": 1,
        # "eta": 0.05,
        # "max_depth": 5,
        # "colsample_bytree": 0.7,
        # "min_child_weight": 5,
        # "subsample": 0.95,
        # "scale_pos_weight": 0.4,
        # "gamma": 0.1,
        # "random_state": 10,
        # "reg_alpha": 1.5,
        # "reg_lambda": 0.1,
    }

    evals = [(dtrain, "train"), (dval, "eval")]
    evals_result = {}
    # Training the model
    clf_xgb = xgb.train(
        params,
        dtrain,
        num_boost_round=500,
        evals=evals,
        evals_result=evals_result,
        early_stopping_rounds=30,
        verbose_eval=False,
    )

    y_proba = clf_xgb.predict(dval)

    logging.info(f"{cv}: Looking for t...")
    # Initialize variables to store the best average F1 score and the corresponding threshold
    best_f1_score = 0
    best_threshold = 0

    # Use precision_recall_curve to get thresholds
    precisions, recalls, thres = metrics.precision_recall_curve(y_val, y_proba)

    n_samples = 100  # Number of thresholds to sample
    if len(thres) > n_samples:
        thres = np.linspace(min(thres), max(thres), n_samples)

    # Initialize arrays to store F1 scores for both positive and negative classes
    f1_scores_pos = np.zeros_like(thres)
    f1_scores_neg = np.zeros_like(thres)

    # Vectorized operation to calculate F1 scores for positive class
    for i, t in enumerate(thres):
        y_pred = (y_proba >= t).astype(int)
        f1_scores_pos[i] = metrics.f1_score(y_val, y_pred)
        f1_scores_neg[i] = metrics.f1_score(y_val, y_pred, pos_label=0)

    # Calculate average F1 scores
    f1_scores_ave = (f1_scores_pos + f1_scores_neg) / 2

    # Find the index of the best average F1 score
    best_index = np.argmax(f1_scores_ave)
    best_f1_score = f1_scores_ave[best_index]
    best_threshold = thres[best_index]

    print(f"Best average F1 score: {best_f1_score}")
    print(f"Best threshold: {best_threshold}")

    y_pred_labels = (y_proba >= 0.5).astype(int)  # Convert probabilities to binary labels
    val_df["pred_proba"] = y_proba
    val_df["pred_labels"] = y_pred_labels
    val_df["prob_loss"] = abs(val_df["pred_proba"] - val_df["label"])
    val_df = val_df[["bID_kec", "label", "pred_labels", "pred_proba", "geometry"]]
    (
        val_df.to_csv(os.path.join(folder_out, f"{cv}_{ver}.csv"), index=False)
        if export_geom == True
        else None
    )

    accuracy = metrics.accuracy_score(y_val, y_pred_labels)
    bal_accuracy = metrics.balanced_accuracy_score(y_val, y_pred_labels)
    f1_score_pos = metrics.f1_score(y_val, y_pred_labels)
    f1_score_neg = metrics.f1_score(y_val, y_pred_labels, pos_label=0)
    f1_score_ave = (f1_score_pos + f1_score_neg) / 2
    recall_pos = metrics.recall_score(y_val, y_pred_labels)
    recall_neg = metrics.recall_score(y_val, y_pred_labels, pos_label=0)
    precision_pos = metrics.precision_score(y_val, y_pred_labels)
    precision_neg = metrics.precision_score(y_val, y_pred_labels, pos_label=0)
    roc_auc = metrics.roc_auc_score(y_val, y_pred_labels)

    try:
        best_iteration = min(
            clf_xgb.best_iteration, len(evals_result["eval"][eval_metric]) - 1
        )
        if eval_metric == "logloss":
            logloss = evals_result["eval"][eval_metric][best_iteration]
            auc = metrics.roc_auc_score(y_val, y_pred_labels)
        else:
            auc = evals_result["eval"][eval_metric][best_iteration]
            logloss = metrics.log_loss(y_val, y_proba)
    except Exception as e:
        logging.warning(f"Error when best iteration: {e}")
        auc = metrics.roc_auc_score(y_val, y_pred_labels)
        logloss = metrics.log_loss(y_val, y_proba)
        best_iteration = "Not using early stopping"

    data = [
        cv,
        best_iteration,
        logloss,
        auc,
        accuracy,
        bal_accuracy,
        f1_score_pos,
        f1_score_neg,
        f1_score_ave,
        recall_pos,
        recall_neg,
        precision_pos,
        precision_neg,
    ]
    report_list.append(data)

report_df = pd.DataFrame(
    report_list,
    columns=[
        "cv",
        "best_iteration",
        "logloss",
        "auc",
        "accuracy",
        "bal_accuracy",
        "f1_score_pos",
        "f1_score_neg",
        "f1_score_ave",
        "recall_pos",
        "recall_neg",
        "precision_pos",
        "precision_neg",
    ],
)

logging.info("Saving results...")
ave_df = report_df.drop(columns=["cv"])
ave_df = ave_df.mean().to_frame().T
ave_df["params"] = [params]
ave_df.to_csv(report_file, header=False, index=False, mode="a")
report_df.to_csv(os.path.join(folder_report, f"initial_model_{ver}.csv"), index=False)
