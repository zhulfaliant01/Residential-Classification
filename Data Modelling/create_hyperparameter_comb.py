import pandas as pd
import numpy as np
import random


def hyperparameter_sampler(hyperparameter):
    hyp = random.sample(hyperparameter, 1)
    return hyp * 5  # Jumlah cv


# Param list
max_depth = [5]
eta = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.15]
gamma = [0.1]
min_child_weight = [5]
colsample_bytree = [0.7]
subsample = [0.95]
scale_pos_weight = [0.4]
reg_lambda = [1.5]
reg_alpha = [0.1]

n = 11

max_depth_ser = []
eta_ser = []
gamma_ser = []
min_child_weights_ser = []
colsample_bytree_ser = []
subsample_ser = []
scale_pos_weight_ser = []
reg_lambda_ser = []
reg_alpha_ser = []
CV = []
for i in range(0, n):
    list_help = hyperparameter_sampler(max_depth)
    max_depth_ser.extend(list_help)

    list_help = hyperparameter_sampler(eta)
    eta_ser.extend(list_help)

    list_help = hyperparameter_sampler(gamma)
    gamma_ser.extend(list_help)

    list_help = hyperparameter_sampler(min_child_weight)
    min_child_weights_ser.extend(list_help)

    list_help = hyperparameter_sampler(colsample_bytree)
    colsample_bytree_ser.extend(list_help)

    list_help = hyperparameter_sampler(subsample)
    subsample_ser.extend(list_help)

    list_help = hyperparameter_sampler(scale_pos_weight)
    scale_pos_weight_ser.extend(list_help)

    list_help = hyperparameter_sampler(reg_lambda)
    reg_lambda_ser.extend(list_help)

    list_help = hyperparameter_sampler(reg_alpha)
    reg_alpha_ser.extend(list_help)

    list_help = [f"CV_{i}"] * 5
    CV.extend(list_help)

# valid_fold
valid_fold = ["cv1_val", "cv2_val", "cv3_val", "cv4_val", "cv5_val"] * n
# df_train
train_fold = ["cv1_train", "cv2_train", "cv3_train", "cv4_train", "cv5_train"] * n

df = pd.DataFrame()

df["CV"] = CV
df["valid_fold"] = valid_fold
df["train_fold"] = train_fold
df["max_depth"] = max_depth_ser
df["eta"] = eta_ser
df["gamma"] = gamma_ser
df["min_child_weight"] = min_child_weights_ser
df["colsample_bytree"] = colsample_bytree_ser
df["subsample"] = subsample_ser
df["scale_pos_weight"] = scale_pos_weight_ser
df["reg_lambda"] = reg_lambda_ser
df["reg_alpha"] = reg_alpha_ser

df.to_csv("Data Modelling\\hyperparameter_lists\\hyperparams_2.csv", index=False)
