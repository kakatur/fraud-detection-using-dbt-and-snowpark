
#
# This model creates machine learning features for swipe data
#

from datetime import datetime
import pandas as pd
import re
from sklearn.preprocessing import StandardScaler

def model(dbt, session):

    ### dbt config

    dbt.config(
        materialized = "incremental",
        unique_key = 'swipe_id',
        packages = ['scikit-learn==1.3.0', 'pandas==1.5.3']
    )

    ### Fetch data from Snowflake (last x days if incremental)

    df = dbt.source("FRAUD_WORKSHOP_DB__SWIPE", "FCT_SWIPE").to_pandas()
    if dbt.is_incremental:
        df = df[df.SWIPE_DATE > datetime.now() - pd.to_timedelta("150day")]

    ### Create features "Swipe Day" and "Swipe Time of Day" from Swipe Date

    df["SWIPE_DAY"] = df["SWIPE_DATE"].apply(lambda x: x.weekday())
    df["SWIPE_TIME_OF_DAY"] = df["SWIPE_DATE"].apply(lambda x: x.hour % 6)

    ### Did the swipe happen in the first week of policy start date?

    df["IS_FIRST_WEEK_OF_POLICY"] = df.apply(lambda row: 1 if (row['SWIPE_DATE']-row['POLICY_START_DATE']).days < 8 else 0, axis=1)

    ### Create feature POSSIBLE_GIFT_CARD from SWIPE_AMOUNT

    df["POSSIBLE_GIFT_CARD"] = df["SWIPE_AMOUNT"].apply(lambda x: 1 if round(x*100) % 100 == 95 or x in [100, 500, 1000] else 0)

    ### Create features SWIPE_COUNT_ROLLING_48_HR and SWIPE_AMOUNT_ROLLING_48_HR for each policy_holder_id

    df_swipe_rolling = (df
        .groupby(['POLICY_HOLDER_ID'])
        .apply(lambda x: x
            .sort_values('SWIPE_DATE')
            .rolling('2D', on='SWIPE_DATE', center=True)
            .agg({'SWIPE_ID': 'count', 'SWIPE_AMOUNT': 'sum'})
        )
        .reset_index()
    )

    df = (df
        .merge(df_swipe_rolling, how='inner', left_index=True, right_on='index', suffixes=['','_ROLLING_48_HR'])
        .rename(columns={'SWIPE_ID_ROLLING_48_HR': 'SWIPE_COUNT_ROLLING_48_HR'})
        .drop(columns='index')
    )

    ### Scale SWIPE_AMOUNT_ROLLING_48_HR

    sc = StandardScaler()
    df["SWIPE_AMOUNT_ROLLING_48_HR"] = sc.fit_transform(df["SWIPE_AMOUNT_ROLLING_48_HR"].values.reshape(-1, 1))

    ### Encode MCC: High Risk MCC will be encoded as 1 and the others as 0

    df["MCC"] = df["MCC"].apply(lambda x: 1 if x in ['5411', '5912', '5300', '5122', '7399'] else 0)

    ### Derive feature MERCHANT_NAME_MATCHES_POLICY_HOLDER_NAME based on how close the merchant name from Policy Holder's First Name and Last Name

    df["MERCHANT_NAME"] = df["MERCHANT_NAME"].apply(lambda x: re.sub(" +", " ", re.sub("[^A-Z\s]+", "", x)).strip())  # remove non-alpha chars
    df["MERCHANT_NAME_MATCHES_POLICY_HOLDER_NAME"] = df.apply(
        lambda row:
                ( len(row['FIRST_NAME']) if row['FIRST_NAME'] in row['MERCHANT_NAME'] else 0 ) +
                ( len(row['FIRST_NAME']) * 3 if row['FIRST_NAME'] + " " in row['MERCHANT_NAME'] else 0 ) +  # more weight for first_name followed by a space
                ( len(row['LAST_NAME']) if row['LAST_NAME'] in row['MERCHANT_NAME'] else 0 ) +
                ( len(row['LAST_NAME']) * 5 if " " + row['LAST_NAME'] in row['MERCHANT_NAME'] else 0 ) +  # even more weight for last_name preceded by a space
                ( ( len(row['FIRST_NAME']) + len(row['LAST_NAME']) ) * 7 if row['FIRST_NAME'] in row['MERCHANT_NAME'] and row['LAST_NAME'] in row['MERCHANT_NAME'] else 0 )
            if row['MERCHANT_NAME'] not in ['WALGREENS']
            else 0,
        axis=1
    )

    ### Encode IS_FRAUD

    df["IS_FRAUD"] = df["IS_FRAUD"].map({'F': 0, 'T': 1}).fillna(0)

    ### Store only features (plus swipe_id, swipe_date and is_fraud)

    features = [
        'SWIPE_ID', 'SWIPE_DATE', 'SWIPE_DAY', 'SWIPE_TIME_OF_DAY', 'IS_FIRST_WEEK_OF_POLICY',
        'POSSIBLE_GIFT_CARD', 'SWIPE_COUNT_ROLLING_48_HR', 'SWIPE_AMOUNT_ROLLING_48_HR',
        'MCC', 'MERCHANT_NAME_MATCHES_POLICY_HOLDER_NAME', 'IS_FRAUD'
    ]
    df = df[features]

    ### Snowpark create_dataframe converts Pandas timestamps into Long numbers :(
    ### The workaround is to localize all timestamps into UTC

    df["SWIPE_DATE"] = df["SWIPE_DATE"].dt.tz_localize('UTC')

    return session.create_dataframe(df)

