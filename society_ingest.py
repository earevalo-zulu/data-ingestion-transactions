import pandas as pd
import uuid
from sqlalchemy import create_engine

df_users_ingest = pd.read_csv('./results/consolidado_review.csv')
df_consolidate_result = pd.read_csv('./results/consolidado_review.csv')

df_users_ingest = df_users_ingest.rename(columns={'id':'id_row'})

#df_users_ingest['final_phone_number'] = df_users_ingest['final_phone_number'].astype('str')
#print('df_users_ingest:', df_users_ingest[df_users_ingest['final_phone_number']!='nan']['final_phone_number'])

df_society_ingest = df_users_ingest.drop_duplicates(subset=['user_id'])

df_society_ingest = df_society_ingest[df_society_ingest['final_entity_type'] == 'company']
df_society_ingest = df_society_ingest[df_society_ingest['user_id'].notna()]

print('df_society_ingest:', df_society_ingest)

df_society_ingest = df_society_ingest[
    [
        'user_id',
        # Location
        'final_country_code',
        'final_state',
        'final_city',
        'final_address',
        # Owner info
        'final_owner_document_type',
        'final_owner_document_number',
        'final_owner_first_name',
        'final_owner_last_name',
        #'final_owner_billing_email',
        # Company info
        'final_document_type',	
        'final_document_number',
        'final_full_name',
        # Others
        'final_email',
        'final_phone_number',
        'created_at_user',
    ]
]

df_society_ingest.rename(
    columns={
        'final_country_code':'country',
        'final_city':'city',
        'final_state': 'state',
        'final_address':'billing_address',
        'final_owner_document_type': 'document_type',
        'final_owner_document_number': 'document_number',
        'final_owner_first_name': 'first_name',
        'final_owner_last_name': 'first_last_name',
        'final_document_type':'company_document_type',
        'final_document_number':'company_document_number',
        'final_full_name': 'company_name',
        'final_email':'billing_email',
        'final_phone_number':'phone',
        'created_at_user':'created_at',
    }, 
    inplace=True
)

print('df_societies:', df_society_ingest)


df_society_ingest['is_american_citizen'] = False
df_society_ingest['active'] = True
df_society_ingest['status'] = 'VERIFIED'
df_society_ingest['main'] = True
df_society_ingest['id'] = None
df_society_ingest['document_type']= None

for index, row in df_society_ingest.iterrows():
    df_society_ingest.at[index, 'id'] = str(uuid.uuid4())
    df_society_ingest.at[index, 'is_american_citizen'] = (str(row['country']) == 'USA')
    company_name = str(row['company_name'])

engine = create_engine('mysql+pymysql://admin:admin@localhost:3306/b2b')
df_society_ingest.to_sql('societies', con=engine, index=False, if_exists='append')

df_ingest_to_consolidate = df_society_ingest.rename(columns={'id':'society_id'})
df_ingest_to_consolidate = df_ingest_to_consolidate[['society_id','user_id']]

df_consolidate_result = df_consolidate_result.merge(df_ingest_to_consolidate, left_on='user_id', right_on='user_id', how='left')
df_consolidate_result.to_csv('./results/consolidado_review.csv')
