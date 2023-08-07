
The included fct_swipe.csv contains ~600,000 production-grade swipe records generated using Python faker module. A number of real fraud scenarios have been coded in the script plus real-like fraud and non-fraud data has been generated for associated data elements as well.

You may run the below Python script to generate the data.

     from datetime import datetime, timedelta
     import pandas as pd
     
     from faker import Faker
     from faker.providers import DynamicProvider
     
     # initialize faker
     fake = Faker()
     
     # add medical professions provider
     fake.add_provider(DynamicProvider(
          provider_name="medical_profession",
          elements=["DR.", "DOCTOR", "NURSE", "SURGEON", "DR", "MD", "PHARMACY", "RX"]
     ))
     
     
     ##### Generate Members
     
     # initialize variables for members
     target_policy_holder_count = 20000
     policy_holder_id = 0
     policy_start_date = datetime(2019, 8, 1)  # we will increment policy_start_date by random minutes for each new member created
     policy_holder_df = pd.DataFrame({
         'is_fraud': [],
         'policy_holder_id': [],
         'first_name': [],
         'last_name': [],
         'policy_start_date': [],
     })
     
     # generate data for members
     for i in range(target_policy_holder_count):
     
         # is_fraud should happen for less than 1% cases
         if fake.random.randint(1, 150) < 2:
             is_fraud = 'T'
         else:
             is_fraud = 'F'
     
         # policy_holder_id is a simple increment
         policy_holder_id += 1
     
         # generate first_name and last_name
         first_name = None
         # fraud members come back 60% of the time; they will have the same name but a different policy_holder_id and policy_start_date
         if is_fraud == 'T':
             if fake.random.randint(1, 100) < 60:
                 fraud_policy_holder_df = policy_holder_df[policy_holder_df["is_fraud"] == 'T'].reset_index()
                 # pick from the existing dataset only if there are atleast 5 members
                 if len(fraud_policy_holder_df.index) > 5:
                     prev_index = fake.random.randint(0, len(fraud_policy_holder_df.index)-1)
                     first_name = fraud_policy_holder_df.loc[prev_index, 'first_name']
                     last_name = fraud_policy_holder_df.loc[prev_index, 'last_name']
         # in all other cases, generate first_name and last_name
         if first_name is None:
             first_name = fake.name().split(' ')[0].upper()
             last_name = fake.name().split(' ')[-1].upper()
     
         # policy_start_date is an increment by random minutes from the previous value
         policy_start_date = policy_start_date + timedelta(minutes=fake.random.randint(1, 180))
         # if the policy_start_date is in the mid-night, add 6 hours
         if policy_start_date.hour < 6:
             policy_start_date = policy_start_date + timedelta(hours=6)
     
         # add to policy_holder_df
         policy_holder_df.loc[len(policy_holder_df.index)] = [is_fraud, policy_holder_id, first_name, last_name, policy_start_date]
     
     # print stats for policy_holder_df
     pd.set_option('display.max_columns', None)
     print(policy_holder_df.head())
     print(policy_holder_df.tail())
     print('fraudster member count: ' + str(len(policy_holder_df[policy_holder_df['is_fraud'] == 'T'].index)))
     print('non-fraudster member count: ' + str(len(policy_holder_df[policy_holder_df['is_fraud'] == 'F'].index)))

     
     ##### Generate Swipes
     
     # initialize variables for swipes
     target_swipe_count = 600000
     swipe_id = 0
     swipe_date = datetime(2020, 1, 1)  # we will increment swipe_date by random seconds for the next swipe
     mcc_master_list = [
         5411, 5912, 5300, 5122, 7399,  # fraud happens mostly with the first 5 types of merchants
         4119, 5047, 5975, 5976, 7277,
         8011, 8031, 8041, 8049, 8050,
         8062, 8071, 8099, 8734, 8021,
         8043, 8042, 5310
     ]
     
     # define lists for each data element
     # these lists will be converted into a pandas dataframe for writing into csv file
     is_fraud_list = []
     swipe_id_list = []
     swipe_date_list = []
     swipe_amount_list = []
     mcc_list = []
     merchant_name_list = []
     merchant_zipcode_list = []
     policy_holder_id_list = []
     first_name_list = []
     last_name_list = []
     policy_start_date_list = []
     
     # generate data for swipes
     for i in range(target_swipe_count):
     
         # swipe_date is incremented by a random number of seconds
         swipe_date = swipe_date + timedelta(seconds=fake.random.randint(1, 120))
     
         # if the swipe_date is in the mid-night, add 6 hours
         if swipe_date.hour < 6:
             swipe_date = swipe_date + timedelta(hours=6)
     
         # selected_policy_holder_df needs to be filled either with a fraudster or non-fraudster member (note: we target 2% of total swipes from fraudsters)
         selected_policy_holder_df = None
     
         # first, let's check if there is any fraudster that has started their policy in the last 30 minutes of swipe_date
         # fraudsters tend to swipe immediately after the policy start date
         filtered_policy_holder_df = policy_holder_df[(
             (policy_holder_df['policy_start_date'] >= swipe_date-timedelta(minutes=30))
             &
             (policy_holder_df['policy_start_date'] < swipe_date)
             &
             (policy_holder_df['is_fraud'] == 'T')
         )]
         if len(filtered_policy_holder_df.index) > 0:
             selected_policy_holder_df = filtered_policy_holder_df.iloc[fake.random.randint(0, len(filtered_policy_holder_df.index)-1)]
     
         # second, select a random member that has their policy started in the last 1 year (fraudster or non-fraudster)
         if selected_policy_holder_df is None:
             filtered_policy_holder_df = policy_holder_df[((policy_holder_df['policy_start_date'] >= swipe_date-timedelta(weeks=56)) & (policy_holder_df['policy_start_date'] < swipe_date))]
             if len(filtered_policy_holder_df.index) > 0:
                 selected_policy_holder_df = filtered_policy_holder_df.iloc[fake.random.randint(0, len(filtered_policy_holder_df.index)-1)]
     
         # if selected_policy_holder_df is still None, raise an error. In this case, we need to generate more member records in the earlier step.
         if selected_policy_holder_df is None:
             print("Swipe Date: " + str(swipe_date))
             raise Exception("Unable to locate a member record")
     
         # generate swipes for fraudsters
         if selected_policy_holder_df['is_fraud'] == 'T':
             fraud_type = fake.random.randint(1, 3)
             if fraud_type == 1:
     
                 # this type of fraud involves swiping the card many times in a short interval
                 for j in range(fake.random.randint(6, 20)):
     
                     # is_fraud
                     is_fraud_list.append(selected_policy_holder_df['is_fraud'])
     
                     # swipe_id is a simple increment
                     swipe_id += 1
                     swipe_id_list.append(swipe_id)
     
                     # swipe_date is incremented by a random number of seconds
                     swipe_date = swipe_date + timedelta(seconds=fake.random.randint(1, 120))
                     swipe_date_list.append(swipe_date)
     
                     # swipe_amount is a random float
                     swipe_amount = fake.random.randint(10000, 500000) / 100
                     swipe_amount_list.append(swipe_amount)
     
                     # for fraud, select one of the first 5 mcc's; for non-fraud, select any
                     mcc = mcc_master_list[fake.random.randint(0, 4)]
                     mcc_list.append(mcc)
     
                     # merchant name
                     merchant_name = fake.medical_profession() + " " + fake.name().upper()
                     merchant_name_list.append(merchant_name)
     
                     # merchant zipcode
                     merchant_zipcode = fake.address().split()[-1]
                     merchant_zipcode_list.append(merchant_zipcode)
     
                     # policy_holder_id
                     policy_holder_id_list.append(selected_policy_holder_df['policy_holder_id'])
     
                     # first_name
                     first_name_list.append(selected_policy_holder_df['first_name'])
     
                     # last_name
                     last_name_list.append(selected_policy_holder_df['last_name'])
     
                     # policy_start_date
                     policy_start_date_list.append(selected_policy_holder_df['policy_start_date'])
     
             elif fraud_type == 2:
     
                 # this type of fraud involves swiping the card with amounts x.95 or 100 or 500
     
                 # is_fraud
                 is_fraud_list.append(selected_policy_holder_df['is_fraud'])
     
                 # swipe_id is a simple increment
                 swipe_id += 1
                 swipe_id_list.append(swipe_id)
     
                 # swipe_date is incremented by a random number of seconds
                 swipe_date = swipe_date + timedelta(seconds=fake.random.randint(1, 180))
                 swipe_date_list.append(swipe_date)
     
                 # swipe_amount is a random float
                 swipe_amount_rand = fake.random.randint(1, 3)
                 if swipe_amount_rand == 1:
                     swipe_amount = fake.random.randrange(100, 6000, 100) + 0.95
                 elif swipe_amount_rand == 2:
                     swipe_amount = fake.random.randrange(100, 6000, 100)
                 else:
                     swipe_amount = fake.random.randrange(500, 6000, 500)
                 swipe_amount_list.append(swipe_amount)
     
                 # for fraud, select one of the first 5 mcc's; for non-fraud, select any
                 mcc = mcc_master_list[fake.random.randint(0, 4)]
                 mcc_list.append(mcc)
     
                 # merchant name
                 merchant_name = fake.medical_profession() + " " + fake.name().upper()
                 merchant_name_list.append(merchant_name)
     
                 # merchant zipcode
                 merchant_zipcode = fake.address().split()[-1]
                 merchant_zipcode_list.append(merchant_zipcode)
     
                 # policy_holder_id
                 policy_holder_id_list.append(selected_policy_holder_df['policy_holder_id'])
     
                 # first_name
                 first_name_list.append(selected_policy_holder_df['first_name'])
     
                 # last_name
                 last_name_list.append(selected_policy_holder_df['last_name'])
     
                 # policy_start_date
                 policy_start_date_list.append(selected_policy_holder_df['policy_start_date'])
     
             elif fraud_type == 3:
     
                 # this type of fraud involves swiping with a merchant that matches member's first_name or last_name
     
                 # is_fraud
                 is_fraud_list.append(selected_policy_holder_df['is_fraud'])
     
                 # swipe_id is a simple increment
                 swipe_id += 1
                 swipe_id_list.append(swipe_id)
     
                 # swipe_date is incremented by a random number of seconds
                 swipe_date = swipe_date + timedelta(seconds=fake.random.randint(1, 180))
                 swipe_date_list.append(swipe_date)
     
                 # swipe_amount is a random float
                 swipe_amount = fake.random.randint(10000, 500000) / 100
                 swipe_amount_list.append(swipe_amount)
     
                 # for fraud, select one of the first 5 mcc's; for non-fraud, select any
                 mcc = mcc_master_list[fake.random.randint(0, 4)]
                 mcc_list.append(mcc)
     
                 # merchant name
                 merchant_name_rand = fake.random.randint(1, 3)
                 if merchant_name_rand == 1:
                     merchant_name = fake.medical_profession() + " " + selected_policy_holder_df['first_name']
                 elif merchant_name_rand == 2:
                     merchant_name = fake.medical_profession() + " " + selected_policy_holder_df['last_name']
                 else:
                     merchant_name = fake.medical_profession() + " " + selected_policy_holder_df['first_name'] + " " + selected_policy_holder_df['last_name']
                 merchant_name_list.append(merchant_name)
     
                 # merchant zipcode
                 merchant_zipcode = fake.address().split()[-1]
                 merchant_zipcode_list.append(merchant_zipcode)
     
                 # policy_holder_id
                 policy_holder_id_list.append(selected_policy_holder_df['policy_holder_id'])
     
                 # first_name
                 first_name_list.append(selected_policy_holder_df['first_name'])
     
                 # last_name
                 last_name_list.append(selected_policy_holder_df['last_name'])
     
                 # policy_start_date
                 policy_start_date_list.append(selected_policy_holder_df['policy_start_date'])
     
         # generate swipes for non-fraudsters
         else:
     
             # is_fraud
             is_fraud_list.append(selected_policy_holder_df['is_fraud'])
     
             # swipe_id is a simple increment
             swipe_id += 1
             swipe_id_list.append(swipe_id)
     
             # swipe_date is incremented by a random number of seconds
             swipe_date = swipe_date + timedelta(seconds=fake.random.randint(1, 180))
             swipe_date_list.append(swipe_date)
     
             # swipe_amount is a random float
             swipe_amount = fake.random.randint(10000, 500000) / 100
             swipe_amount_list.append(swipe_amount)
     
             # for fraud, select one of the first 5 mcc's; for non-fraud, select any
             mcc = mcc_master_list[fake.random.randint(0, len(mcc_master_list)-1)]
             mcc_list.append(mcc)
     
             # merchant name
             merchant_name = fake.medical_profession() + " " + fake.name().upper()
             merchant_name_list.append(merchant_name)
     
             # merchant zipcode
             merchant_zipcode = fake.address().split()[-1]
             merchant_zipcode_list.append(merchant_zipcode)
     
             # policy_holder_id
             policy_holder_id_list.append(selected_policy_holder_df['policy_holder_id'])
     
             # first_name
             first_name_list.append(selected_policy_holder_df['first_name'])
     
             # last_name
             last_name_list.append(selected_policy_holder_df['last_name'])
     
             # policy_start_date
             policy_start_date_list.append(selected_policy_holder_df['policy_start_date'])
     
     
     # create dataframe from lists
     df = pd.DataFrame({
         'is_fraud': is_fraud_list,
         'swipe_id': swipe_id_list,
         'swipe_date': swipe_date_list,
         'swipe_amount': swipe_amount_list,
         'mcc': mcc_list,
         'merchant_name': merchant_name_list,
         'merchant_zipcode': merchant_zipcode_list,
         'policy_holder_id': policy_holder_id_list,
         'first_name': first_name_list,
         'last_name': last_name_list,
         'policy_start_date': policy_start_date_list,
     })
     
     # dump data into a csv file
     pd.set_option('display.max_columns', None)
     print(df.head())
     print(df.tail())
     print('fraudster swipe count: ' + str(len(df[df['is_fraud'] == 'T'].index)))
     print('non-fraudster swipe count: ' + str(len(df[df['is_fraud'] == 'F'].index)))
     df.to_csv("fct_swipe.csv", index=False, chunksize=5000)
