
--
-- Run the below SQL statements in Snowsight SQL Worksheet
--

CREATE DATABASE fraud_workshop_db;

CREATE SCHEMA fraud_workshop_db.swipe;

CREATE STAGE fraud_workshop_db.public.models_stage;

CREATE TABLE fraud_workshop_db.swipe.fct_swipe (
	is_fraud		VARCHAR(1),
        swipe_id		INTEGER,
        swipe_date		TIMESTAMP_NTZ,
        swipe_amount		FLOAT,
        mcc			VARCHAR(4),
        merchant_name		VARCHAR(100),
        merchant_zipcode	VARCHAR(5),
        policy_holder_id	INTEGER,
        first_name		VARCHAR(50),
        last_name		VARCHAR(50),
        policy_start_date	TIMESTAMP_NTZ
);

