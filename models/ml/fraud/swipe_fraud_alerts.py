
#
# This model builds the machine learning model for swipe fraud alerts and uploads the model file into Snowflake stage area.
#

from datetime import datetime
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, f1_score

import sys
import os
import joblib
import cachetools

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import FloatType, TimestampType, StringType, StructType, StructField

def model(dbt, session):

    ### dbt config

    dbt.config(
        materialized = "incremental",
        packages = ['scikit-learn==1.2.2', 'xgboost==1.7.3', 'cachetools==4.2.2', 'pandas==1.5.3']
    )

    ### Load features from Snowflake

    df_dim_swipe_features = dbt.ref("dim_swipe_features").to_pandas()

    ### Exclude the latest 2 days from the dataset

    df_dim_swipe_features = df_dim_swipe_features[df_dim_swipe_features.SWIPE_DATE < datetime.now() - pd.to_timedelta("2day")]

    ### Since only 0.2% of the swipe data has been from the known fraudsters (good for us!),
    ### for balance, we select all fraud swipes, followed by the same number of latest non-fraud swipes.
    ### Taking the same number of non-fraud swipes resulted in more swipes being flagged, so bumping the non-fraud swipes to 3 x fraud swipes.

    df_fraud = df_dim_swipe_features[df_dim_swipe_features.IS_FRAUD == 1]
    df_non_fraud = df_dim_swipe_features[df_dim_swipe_features.IS_FRAUD == 0].sort_values('SWIPE_ID').tail(len(df_fraud.index)*3)
    df = pd.concat([df_fraud, df_non_fraud])

    ### Define features and target

    features = [
        'SWIPE_DAY', 'SWIPE_TIME_OF_DAY', 'IS_FIRST_WEEK_OF_POLICY',
        'POSSIBLE_GIFT_CARD', 'SWIPE_COUNT_ROLLING_48_HR', 'SWIPE_AMOUNT_ROLLING_48_HR',
        'MERCHANT_MCC', 'MERCHANT_NAME_MATCHES_MEMBER_NAME', 'SWIPE_CITY_FRAUDSTER_COUNT'
    ]
    target = 'IS_FRAUD'

    ### Split the data into train and test datasets

    X = df[features]
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y)

    ### Initialize the variables

    model_accuracy_score = 0
    model_f1_score = 0
    swipe_fraud_alerts_model_status = 'not_deployed'

    ### Train using multiple models and select the best

    for m in ['DecisionTreeClassifier', 'KNeighborsClassifier', 'LogisticRegression', 'SVC', 'RandomForestClassifier', 'XGBClassifier']:

        if m == 'DecisionTreeClassifier':
            swipe_fraud_alerts_model_tmp = DecisionTreeClassifier(max_depth = 4, criterion = 'entropy').fit(X_train, y_train)
        elif m == 'KNeighborsClassifier':
            swipe_fraud_alerts_model_tmp = KNeighborsClassifier(n_neighbors = 5).fit(X_train, y_train)
        elif m == 'LogisticRegression':
            swipe_fraud_alerts_model_tmp = LogisticRegression().fit(X_train, y_train)
        elif m == 'SVC':
            swipe_fraud_alerts_model_tmp = SVC().fit(X_train, y_train)
        elif m == 'RandomForestClassifier':
            swipe_fraud_alerts_model_tmp = RandomForestClassifier(max_depth = 4).fit(X_train, y_train)
        elif m == 'XGBClassifier':
            swipe_fraud_alerts_model_tmp = XGBClassifier(max_depth = 4).fit(X_train, y_train)

        y_pred = swipe_fraud_alerts_model_tmp.predict(X_test)
        model_accuracy_score_tmp = accuracy_score(y_test, y_pred)
        model_f1_score_tmp = f1_score(y_test, y_pred, average="macro")

        if model_accuracy_score_tmp > model_accuracy_score and model_f1_score_tmp > model_f1_score:
            swipe_fraud_alerts_model_name = m
            swipe_fraud_alerts_model = swipe_fraud_alerts_model_tmp
            model_accuracy_score = model_accuracy_score_tmp
            model_f1_score = model_f1_score_tmp

    ### Fetch accuracy_score and f1_score from the previous run

    if dbt.is_incremental:
        previous_scores_sql = f"""
            select accuracy_score, f1_score from {dbt.this}
            where model_creation_time_utc = (select max(model_creation_time_utc) from {dbt.this} where status = 'deployed')
        """
        previous_scores = session.sql(previous_scores_sql).collect()[0]
        previous_accuracy_score = previous_scores[0]
        previous_f1_score = previous_scores[1]
    else:
        previous_accuracy_score = 0
        previous_f1_score = 0

    ### Publish new version of the model if the accuracy scores have been improved

    if model_accuracy_score > previous_accuracy_score and model_f1_score > previous_f1_score:

        ### Store model into Snowflake stage

        joblib.dump(swipe_fraud_alerts_model, '/tmp/swipe_fraud_alerts_model.joblib', compress=True)
        session.file.put('/tmp/swipe_fraud_alerts_model.joblib', "@"+dbt.this.database+".PUBLIC.MODELS_STAGE", auto_compress=False, overwrite=True)

        ### Import model into the session for UDF

        session.add_import("@"+dbt.this.database+".PUBLIC.MODELS_STAGE/swipe_fraud_alerts_model.joblib")

        ### Define load_file function that also caches the file

        @cachetools.cached(cache={})
        def load_file(filename):
            import_dir = sys._xoptions.get("snowflake_import_directory")
            with open(os.path.join(import_dir, filename), "rb") as f:
                return joblib.load(f)

        ### Create UDF

        @udf(name='predict_fraudster_swipe', is_permanent=False, stage_location = "@"+dbt.this.database+".PUBLIC.MODELS_STAGE", replace=True)
        def predict_fraudster_swipe(args: list) -> float:

            swipe_fraud_alerts_model = load_file("swipe_fraud_alerts_model.joblib")
            df = pd.DataFrame([args], columns=features)
            return swipe_fraud_alerts_model.predict_proba(df)[:,1]

        ### Set model_status to deployed

        swipe_fraud_alerts_model_status = 'deployed'

    ### Return model statitics

    return session.create_dataframe(
        [(datetime.now(), model_accuracy_score, model_f1_score, swipe_fraud_alerts_model_status, swipe_fraud_alerts_model_name)],
        schema = StructType([
            StructField("MODEL_CREATION_TIME_UTC", TimestampType()),
            StructField("ACCURACY_SCORE", FloatType()),
            StructField("F1_SCORE", FloatType()),
            StructField("STATUS", StringType()),
            StructField("MODEL_NAME", StringType()),
        ])
    )
