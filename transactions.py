import pandas as pd
import requests
import uuid
    
def create_order(order):
    transactionId = ""
    orderId = ""
    try:    
        orderResponse = requests.post('http://localhost:8080/zulu-b2b-api/v1/write/orders', json=order)
        orderResponse = orderResponse.json()
        print('OrderResponse:',orderResponse)
        orderId = orderResponse['id']
        transactions = orderResponse['transactions']
        transactionId = transactions[0]['id']
        return orderId, transactionId, orderResponse
    except Exception as e:
        print(f"Error al crear la orden: {e}{orderResponse}")
        return orderId, transactionId, orderResponse

def get_countries():
    countries = {}
    countries['Argentina'] = 'ARG'
    #countries['Blockchain'] = ''
    countries['Brasil'] = 'BRS'
    countries['Chile'] = 'CHL'
    countries['Colombia'] = 'COL'
    countries['Ecuador'] = 'ECU'
    #countries['Europe'] = ''
    countries['Mexico'] = 'MEX'
    countries['Mexico (USD)'] = 'MEX'
    #countries['Panamá'] = ''
    countries['Peru'] = 'PER'
    countries['Peru (USD)'] = 'PER'
    # countries['United Kingdom'] = ''
    countries['United States'] = 'USA'
    countries['Venezuela'] = 'VEN'
    #countries['Zulu Wallet'] = ''
    return countries

def get_currencies():
    currencies = {}
    currencies['COP']='COP'
    currencies['MXN']='MXN'
    currencies['USD']='USD'
    currencies['SOL']='PEN'
    currencies['USDC']='USDC'
    currencies['PEN']='PEN'
    #currencies['BRL']=
    #currencies['CUSD']=
    currencies['ARS']='ARS'
    #currencies['GBP']=
    #currencies['EUR']=
    currencies['CLP']='CLP'
    currencies['BS']='VES'
    return currencies


countries = get_countries()
currencies = get_currencies()

df_transactions = pd.read_csv('zulu_transfer_data_20230918.csv')

# Se eliminan registros ducplicados de data
df_transactions = df_transactions.drop_duplicates()

order_ids = []
transaction_ids = []

df_transactions['orderId'] = None
df_transactions['transactionId'] = None
df_transactions['observation'] = None

for _, transaction in df_transactions.iterrows():
    order = {}

    transactionId = str(transaction.get('trx_id'))
    if len(transactionId ) == 20:
        df_transactions.at[_, 'observation'] = 'Transacción ya existente'
        continue

    sourceCountry = str(transaction.get('origin_country')).strip()
    sourceCountryCode = countries.get(sourceCountry)
    if not sourceCountryCode:
        df_transactions.at[_, 'observation'] = 'Problemas con el sourceCountry - origin_country'
        continue

    targetCountry = transaction.get('destiny_country').strip()
    targetCountryCode = countries.get(targetCountry)
    if not targetCountryCode:
        df_transactions.at[_, 'observation'] = 'Problemas con el targetCountry - destiny_country'
        continue

    sourceCurrency = transaction.get('origin_currency').strip()
    sourceCurrencyCode = currencies.get(sourceCurrency)
    if not sourceCurrencyCode:
        df_transactions.at[_, 'observation'] = 'Problemas con el sourceCurrency - origin_currency'
        continue

    targetCurrency = transaction.get('destiny_currency').strip()
    targetCurrencyCode = currencies.get(targetCurrency)
    if not targetCurrencyCode:
        df_transactions.at[_, 'observation'] = 'Problemas con el targetCurrency - destiny_currency'
        continue

    sourceAmount = str(transaction.get('ci_fiat_value')).strip()
    if sourceAmount == "nan":
        df_transactions.at[_, 'observation'] = 'Problemas con el sourceAmount - ci_fiat_value'
        continue

    sourceRateOperation = str(transaction.get('ci_zulu_rate')).strip()
    if sourceRateOperation == "nan":
        df_transactions.at[_, 'observation'] = 'Problemas con el sourceRateOperation - ci_zulu_rate'
        continue

    sourceAmountUsd = str(transaction.get('ci_USDC_value')).strip()
    if sourceAmountUsd == "nan":
        df_transactions.at[_, 'observation'] = 'Problemas con el sourceAmountUsd - ci_USDC_value'
        continue

    targetAmount = str(transaction.get('co_fiat_value')).strip()
    if targetAmount == "nan":
        df_transactions.at[_, 'observation'] = 'Problemas con el targetAmount - co_fiat_value'
        continue

    targetAmountUsd = str(transaction.get('co_usdc_value')).strip()
    if targetAmountUsd == "nan":
        df_transactions.at[_, 'observation'] = 'Problemas con el targetAmountUsd - co_usdc_value'
        continue

    targetRate = str(transaction.get('co_zulu_rate')).strip()
    if targetRate == "nan":
        df_transactions.at[_, 'observation'] = 'Problemas con el targetRate - co_zulu_rate'
        continue
    
    transactions = []
    transaction01={}

    userId = str(uuid.uuid4())
    order['userId'] = userId
    
    order['active'] = True
    order['cashInPaymentMethod'] = "bank_transfer"
    if transaction['user_type'] == "B2B":
        order['societyId'] = str(uuid.uuid4())
        order['entityType'] = 'company'
    else:
        order['naturalPersonId'] = str(uuid.uuid4())
        order['entityType'] = 'person'
    order['sourceCountry'] = sourceCountryCode
    order['voucherCashIn'] = 'ingesta'
    
    order['sourceCurrency'] = sourceCurrencyCode

    sourceAmount = float(sourceAmount.replace('$', '').replace(',', ''))
    order['sourceAmount'] = sourceAmount
    transaction01['sourceAmount'] = sourceAmount
    
    sourceAmountUsd = float(sourceAmountUsd.replace('$', '').replace(',', ''))
    order['sourceAmountUsd'] = sourceAmountUsd
    transaction01['sourceAmountUsd'] = sourceAmountUsd
    
    sourceRateOperation = float(sourceRateOperation.replace('$', '').replace(',', ''))
    order['sourceRate'] = sourceRateOperation
    order['sourceRateOperation'] = sourceRateOperation
    transaction01['sourceRate'] = sourceRateOperation
    transaction01['sourceRateOperation'] = sourceRateOperation

    transaction01["contactId"] = str(uuid.uuid4())
    transaction01["sourceCountry"]= sourceCountryCode
    transaction01["sourceCurrency"]= sourceCurrencyCode
    transaction01["targetCurrency"]= targetCurrencyCode
    transaction01["targetCountry"]= targetCountryCode
    transaction01["userId"]= userId
    transaction01["accountBankNameSource"]= "Banco"
    transaction01["accountNumberSource"]= "999999999999"
    transaction01["accountBankNameTarget"]= "Banco"
    transaction01["accountNumberTarget"]= "999999999999"
    transaction01["active"]= True
    transaction01["status"]= "COMPLETED"
    
    targetAmount = float(targetAmount.replace('$', '').replace(',', ''))
    transaction01["targetAmount"]= targetAmount

    targetAmountUsd = float(targetAmountUsd.replace('$', '').replace(',', ''))
    transaction01["targetAmountUsd"]= targetAmountUsd

    targetRate = float(targetRate.replace('$', '').replace(',', ''))
    transaction01["targetRate"]= targetRate

    # Pendientes
    order["cashInCommission"]= 0
    order["cashOutCommission"]= 0
    transaction01["sourceAmountFactor"]= 1
    transaction01["sourceAmountFactorUsd"]= 1
    #########################
    

    kam = str(transaction.get('KAM'))
    if kam == "nan":
        transaction01['comment']= 'Ingesta Transacciones'
    else:
        transaction01['comment']= 'Ingesta Transacciones - ' + kam

    transactions.append(transaction01)
    order['transactions'] = transactions

    
    orderId, transactionId, orderResponse = create_order(order)
    if orderId == "":
        df_transactions.at[_, 'observation'] = 'Problemas: '+ str(orderResponse)

    order_ids.append(orderId)
    transaction_ids.append(transactionId)

    df_transactions.at[_, 'orderId'] = orderId
    df_transactions.at[_, 'transactionId'] = transactionId

df_transactions.to_csv('zulu_transfer_data_20230918_result.csv', index=False)
