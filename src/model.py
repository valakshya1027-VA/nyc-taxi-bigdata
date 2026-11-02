"""Stage 4: train/evaluate Linear Regression, Random Forest and Gradient Boosted Trees."""
import time
from pathlib import Path

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, functions as F

from src import config

NUMERIC = ["hour", "dow", "month", "is_weekend", "is_holiday",
           "lag_1h", "lag_24h", "lag_168h", "roll_mean_3h", "roll_mean_24h"]


def time_split(demand: DataFrame):
    """Chronological split: the last TEST_DAYS days are the test set (no random shuffling)."""
    max_ts = demand.agg(F.max("hour_ts")).collect()[0][0]
    cutoff = F.lit(max_ts) - F.expr(f"INTERVAL {config.TEST_DAYS} DAYS")
    return demand.filter(F.col("hour_ts") < cutoff), demand.filter(F.col("hour_ts") >= cutoff)


def _pipeline(regressor, onehot: bool) -> Pipeline:
    idx = StringIndexer(inputCol="zone_str", outputCol="zone_idx", handleInvalid="keep")
    stages = [idx]
    cols = list(NUMERIC)
    if onehot:   # linear model needs one-hot; trees can use the index directly
        stages.append(OneHotEncoder(inputCols=["zone_idx"], outputCols=["zone_vec"], handleInvalid="keep"))
        cols.append("zone_vec")
    else:
        cols.append("zone_idx")
    stages += [VectorAssembler(inputCols=cols, outputCol="features", handleInvalid="skip"), regressor]
    return Pipeline(stages=stages)


def run_models(demand: DataFrame) -> pd.DataFrame:
    demand = demand.withColumn("zone_str", F.col("PULocationID").cast("string"))
    train, test = time_split(demand)
    train.cache()
    test.cache()
    print(f"train rows: {train.count():,}  test rows: {test.count():,}")

    common = dict(featuresCol="features", labelCol="pickups")
    models = {
        "LinearRegression": _pipeline(LinearRegression(**common, regParam=0.01), onehot=True),
        "RandomForest": _pipeline(RandomForestRegressor(**common, numTrees=60, maxDepth=10, maxBins=300, seed=config.SEED), onehot=False),
        "GradientBoostedTrees": _pipeline(GBTRegressor(**common, maxIter=60, maxDepth=6, maxBins=300, seed=config.SEED), onehot=False),
    }

    rows, best = [], None
    for name, pipe in models.items():
        t0 = time.time()
        fitted = pipe.fit(train)
        train_s = time.time() - t0
        pred = fitted.transform(test)
        metrics = {m: RegressionEvaluator(labelCol="pickups", predictionCol="prediction", metricName=m).evaluate(pred)
                   for m in ("rmse", "mae", "r2")}
        rows.append({"model": name, **{k: round(v, 4) for k, v in metrics.items()}, "train_seconds": round(train_s, 1)})
        print(rows[-1])
        if best is None or metrics["rmse"] < best[1]:
            best = (name, metrics["rmse"], fitted, pred)

    results = pd.DataFrame(rows)
    out = Path(config.OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    results.to_csv(out / "model_results.csv", index=False)

    # Feature importance (tree models only)
    name, _, fitted, pred = best
    est = fitted.stages[-1]
    if hasattr(est, "featureImportances"):
        names = NUMERIC + ["zone_idx"]
        imp = pd.DataFrame({"feature": names, "importance": list(est.featureImportances.toArray())})
        imp.sort_values("importance", ascending=False).to_csv(out / "feature_importance.csv", index=False)

    # Predictions for the 5 busiest zones (used by the dashboard)
    top_zones = [r[0] for r in test.groupBy("PULocationID").agg(F.sum("pickups").alias("s")).orderBy(F.desc("s")).limit(5).collect()]
    (pred.filter(F.col("PULocationID").isin(top_zones))
         .select("PULocationID", "hour_ts", "pickups", "prediction").orderBy("PULocationID", "hour_ts")
         .toPandas().to_csv(out / "predictions.csv", index=False))

    # Error by hour of day for the best model
    (pred.withColumn("abs_err", F.abs(F.col("pickups") - F.col("prediction")))
         .groupBy("hour").agg(F.avg("abs_err").alias("mae")).orderBy("hour")
         .toPandas().to_csv(out / "error_by_hour.csv", index=False))

    print(f"best model: {name}")
    return results
