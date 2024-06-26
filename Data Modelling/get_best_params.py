import pandas as pd

file = r"Data Modelling\randomized_hyperparameter_results_2.csv"

df = pd.read_csv(file)

w1 = 0.65
w2 = 1 - w1

df["new"] = w1 * df["accuracy"] + w2 * df["balanced_accuracy"]

metrics = "eval_auc"

best = df.groupby(by=["cv"])[metrics].mean().sort_values(ascending=False)
best_index = best.head(20).index
print(best.head(20))

fin = df[df.cv.isin(best_index)].sort_values(by=["cv", "cv_num"])
fin.to_csv(f"Data Modelling\\hasil_2 {metrics}.csv", index=False)
