import pandas as pd
import os
import logging
import xgboost as xgb
import traceback
from sklearn import metrics
from alive_progress import alive_bar

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

hyperparams = pd.read_csv("Data Modelling\\hyperparameter_lists\\hyperparams_2.csv")
# Path to save results
results_csv_path = "Data Modelling\\randomized_hyperparameter_results_2.csv"
results_columns = [
    "cv",
    "cv_num",
    "eta",
    "max_depth",
    "gamma",
    "min_child_weight",
    "colsample_bytree",
    "subsample",
    "scale_pos_weight",
    "reg_lambda",
    "reg_alpha",
    "best_iteration",
    "train_auc",
    "eval_auc",
    "accuracy",
    "balanced_accuracy",
    "f1_score_pos",
    "f1_score_neg",
    "f1_score_ave",
    "recall",
    "specificity",
    "roc_auc",
    "logloss",
]

if not os.path.exists(results_csv_path):
    results_df = pd.DataFrame(columns=results_columns)
    results_df.to_csv(results_csv_path, index=False)

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

hyperparams.sort_values(by=["valid_fold", "CV"], inplace=True)
print(hyperparams.head(10))
with alive_bar(len(hyperparams)) as bar:
    last_data = ""
    for _, row in hyperparams.iterrows():
        try:
            cv = row.CV
            cv_num = row.valid_fold.replace("cv", "").replace("_val", "")
            logging.info(f"{cv}-{cv_num}: start...")

            if last_data != row.valid_fold:
                last_data = row.valid_fold
                path = r"Dataset\6_merged\training_cv\5_fold"
                valid_df = pd.read_csv(os.path.join(path, row.valid_fold + ".csv"))
                train_df = pd.read_csv(os.path.join(path, row.train_fold + ".csv"))

                try:
                    train_df = train_df.drop(columns=dropped_columns, axis=1)
                except:
                    valid_df = valid_df.drop(columns=dropped_columns, axis=1)

                valid_df = valid_df[train_df.columns]  # to match the training columns

                X_train = train_df.drop(
                    columns=["bID_kec", "label", "geometry"], axis=1
                ).copy()
                y_train = train_df[["label"]].copy()

                X_val = valid_df.drop(columns=["bID_kec", "label", "geometry"], axis=1).copy()
                y_val = valid_df[["label"]].copy()

                # Creating DMatrix for training and validation sets
                dtrain = xgb.DMatrix(data=X_train, label=y_train)
                dval = xgb.DMatrix(data=X_val, label=y_val)

            params = {
                "objective": "binary:logistic",
                "eval_metric": "auc",
                "eta": row.eta,
                "max_depth": row.max_depth,
                "gamma": row.gamma,
                "min_child_weight": row.min_child_weight,
                "colsample_bytree": row.colsample_bytree,
                "subsample": row.subsample,
                "scale_pos_weight": row.scale_pos_weight,
                "reg_lambda": row.reg_lambda,
                "reg_alpha": row.reg_alpha,
            }

            evals = [(dtrain, "train"), (dval, "eval")]
            evals_result = {}
            clf_xgb = xgb.train(
                params,
                dtrain,
                evals=evals,
                num_boost_round=500,
                evals_result=evals_result,
                early_stopping_rounds=30,
                verbose_eval=False,
            )

            y_pred = clf_xgb.predict(dval)
            y_pred_labels = (y_pred >= 0.5).astype(
                int
            )  # Convert probabilities to binary labels

            accuracy = metrics.accuracy_score(y_val, y_pred_labels)
            bal_accuracy = metrics.balanced_accuracy_score(y_val, y_pred_labels)
            f1_score_pos = metrics.f1_score(y_val, y_pred_labels)
            f1_score_neg = metrics.f1_score(y_val, y_pred_labels, pos_label=0)
            f1_score_ave = (f1_score_pos + f1_score_neg) / 2
            recall = metrics.recall_score(y_val, y_pred_labels)
            specificity = metrics.recall_score(y_val, y_pred_labels, pos_label=0)
            roc_auc = metrics.roc_auc_score(y_val, y_pred_labels)
            logloss = metrics.log_loss(y_val, y_pred)

            # Ensure best_iteration is within the bounds of the evals_result lists
            best_iteration = min(clf_xgb.best_iteration, len(evals_result["eval"]["auc"]) - 1)

            best_train_auc = evals_result["train"]["auc"][best_iteration]
            best_eval_auc = evals_result["eval"]["auc"][best_iteration]

            best_results = {
                "cv": cv,
                "cv_num": cv_num,
                "eta": row.eta,
                "max_depth": row.max_depth,
                "gamma": row.gamma,
                "min_child_weight": row.min_child_weight,
                "colsample_bytree": row.colsample_bytree,
                "subsample": row.subsample,
                "scale_pos_weight": row.scale_pos_weight,
                "reg_lambda": row.reg_lambda,
                "reg_alpha": row.reg_alpha,
                "best_iteration": best_iteration,
                "train_auc": best_train_auc,
                "eval_auc": best_eval_auc,
                "accuracy": accuracy,
                "balanced_accuracy": bal_accuracy,
                "f1_score_pos": f1_score_pos,
                "f1_score_neg": f1_score_neg,
                "f1_score_ave": f1_score_ave,
                "recall": recall,
                "specificity": specificity,
                "roc_auc": roc_auc,
                "logloss": logloss,
            }

            # Append to results CSV
            results_df = pd.DataFrame([best_results])
            results_df.to_csv(results_csv_path, mode="a", header=False, index=False)
        except Exception as e:
            logging.info(f"{cv}-{cv_num}: Error - {e}")
            traceback.print_exc()
            continue

        bar()
