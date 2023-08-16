{% docs ml_models %}

ML model scripts in this directory create database tables with model accuracy statistics data from each run.

The below Snowflake stages need to be created before running these ml models:

- create stage DATA_SCIENCE.PUBLIC.MODELS_STAGE;
- grant all on stage DATA_SCIENCE.PUBLIC.MODELS_STAGE to ETL_ROLE;

ML models run incrementally by default. To run a full-load of ML models:

- dbt run --full-refresh --select models/ds_models/fraud/{model_name}

{% enddocs %}
