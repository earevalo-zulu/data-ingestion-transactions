import pandas as pd
import uuid
from sqlalchemy import create_engine

df_consolidate = pd.read_csv('./results/consolidado.csv')
df_consolidate_result = pd.read_csv('./results/consolidado.csv')

df_consolidate['status'] = 'VERIFIED'
df_consolidate['active'] = True
df_consolidate['on_boarding_step'] = 0
df_consolidate['avoid_person_liquidity_providers'] = 0

df_consolidate = df_consolidate[df_consolidate['final_email'].notna()]

df_consolidate['date'] = pd.to_datetime(df_consolidate['date'], format='%m/%d/%Y')
df_consolidate['date'] = df_consolidate['date'].dt.strftime('%Y-%m-%d 00:00:00')

df_created_at = df_consolidate[['final_email','final_created_at']]
df_date = df_consolidate[['final_email', 'date']].rename(columns={'date':'final_created_at'})
df_union_created_at = pd.concat([df_created_at, df_date], ignore_index=True)

print('DF_Union_Created:',df_union_created_at)

df_email_minimum = df_union_created_at.groupby('final_email', as_index=False)['final_created_at'].min().rename(columns={'final_created_at':'final_created_at_new'})

print('df_email_minimum:',df_email_minimum)

df_consolidate = df_consolidate.drop_duplicates(subset=['final_email'])

df_consolidate = df_consolidate.merge(df_email_minimum, on='final_email', how='left')

df_consolidate.rename(columns={'id':'id_row'}, inplace=True)

df_consolidate['id'] = None
for index, row in df_consolidate.iterrows():
    df_consolidate.at[index, 'id'] = str(uuid.uuid4())

select_fields = [
    'id',
    #'id_row',
    'final_email',
    'status',
    'final_entity_type',
    'on_boarding_step',
    'final_country_code',
    'active',
    #'final_created_at',
    'final_created_at_new',
    'avoid_person_liquidity_providers',
    'sales_representative',
]
df_ingest = df_consolidate[select_fields]

new_names = {
    'final_entity_type': 'entity_type',
    'final_email': 'email',
    'final_country_code': 'country_code',
    'final_country': 'country',
    'final_created_at_new': 'created_at',
}
df_ingest = df_ingest.rename(columns=new_names)


engine = create_engine('mysql+pymysql://admin:admin@localhost:3306/b2b')

# if_exists 'replace' <-> 'append'
#df_consolidate.to_sql('users', con=engine, index=False, if_exists='replace')
df_ingest.to_sql('users', con=engine, index=False, if_exists='append')
df_ingest.to_csv('./results/ingest_users.csv')

df_ingest_to_consolidate = df_ingest.rename(columns={'id':'user_id','created_at':'created_at_user'})
df_ingest_to_consolidate = df_ingest_to_consolidate[['email','user_id','created_at_user']]

df_consolidate_result = df_consolidate_result.merge(df_ingest_to_consolidate, left_on='final_email', right_on='email', how='left')
del df_consolidate_result['email']
df_consolidate_result.to_csv('./results/consolidado_review.csv')
