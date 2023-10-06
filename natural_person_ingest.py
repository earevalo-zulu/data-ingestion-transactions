import pandas as pd
import uuid
from sqlalchemy import create_engine

df_users_ingest = pd.read_csv('./results/consolidado_review.csv')
df_consolidate_result = pd.read_csv('./results/consolidado_review.csv')

df_users_ingest = df_users_ingest.rename(columns={'id':'id_row'})

#df_users_ingest['final_phone_number'] = df_users_ingest['final_phone_number'].astype('str')
#print('df_users_ingest:', df_users_ingest[df_users_ingest['final_phone_number']!='nan']['final_phone_number'])


df_natural_person_ingest = df_users_ingest.drop_duplicates(subset=['user_id'])

#df_company = df_natural_person_ingest

print('Cantidad de registros:', df_natural_person_ingest.shape[0])

df_natural_person_ingest = df_natural_person_ingest[df_natural_person_ingest['final_entity_type'] == 'person']
#df_company = df_company[df_company['final_entity_type'] == 'company']

print('Cantidad de registros (person):', df_natural_person_ingest.shape[0])

df_natural_person_ingest = df_natural_person_ingest[
    [
        'user_id',
        'final_country_code',
        'final_state',
        'final_city',
        'final_address',
        'final_document_type',	
        'final_document_number',
        'final_first_name',	
        'final_last_name',
        'final_full_name',
        'final_email',
        'final_phone_number',
        'created_at_user',
    ]
]

df_natural_person_ingest.rename(
    columns={
        'final_country_code':'country',
        'final_state': 'state',
        'final_city':'city',
        'final_address':'billing_address',
        'final_document_type':'document_type',
        'final_document_number':'document_number',
        'final_first_name': 'first_name',
        'final_last_name': 'first_last_name',
        'final_full_name': 'full_name',
        'final_email':'billing_email',
        'final_phone_number':'phone',
        'created_at_user':'created_at',
    }, 
    inplace=True
)

df_natural_person_ingest['active'] = True
df_natural_person_ingest['is_american_citizen'] = False
df_natural_person_ingest['status'] = 'VERIFIED'

print('df_natural_person_ingest:',df_natural_person_ingest)

df_natural_person_ingest['id'] = None
for index, row in df_natural_person_ingest.iterrows():
    df_natural_person_ingest.at[index, 'id'] = str(uuid.uuid4())
    df_natural_person_ingest.at[index, 'is_american_citizen'] = (str(row['country']) == 'USA')


engine = create_engine('mysql+pymysql://admin:admin@localhost:3306/b2b')
df_natural_person_ingest.to_sql('natural_persons', con=engine, index=False, if_exists='append')

df_ingest_to_consolidate = df_natural_person_ingest.rename(columns={'id':'natural_person_id'})
df_ingest_to_consolidate = df_ingest_to_consolidate[['natural_person_id','user_id']]

df_consolidate_result = df_consolidate_result.merge(df_ingest_to_consolidate, left_on='user_id', right_on='user_id', how='left')
df_consolidate_result.to_csv('./results/consolidado_review.csv')
