import datetime
import requests
import json
import decimal
import petl
import psycopg2


def bank_of_canada():
    start_date = '2020-01-01'
    url = 'https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json?start_date='
    request = requests.get(url+start_date)
    bod = json.loads(request.text)

    FXUSDCAD_rates = []
    FXAUDCAD_rates = []
    FXEURCAD_rates = []
    dates = []

    for row in bod['observations']:
        dates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        FXUSDCAD_rates.append(decimal.Decimal(row['FXUSDCAD']['v']))
        FXEURCAD_rates.append(decimal.Decimal(row['FXEURCAD']['v']))
        FXAUDCAD_rates.append(decimal.Decimal(row['FXAUDCAD']['v']))

    exchange_rates = petl.fromcolumns([dates, FXUSDCAD_rates, FXEURCAD_rates, FXAUDCAD_rates],
                                      header=['dates', 'fxusdcad_rates', 'fxeurcad_rates', 'fxaudcad'])
    print(exchange_rates)

    try:
        conn = psycopg2.connect(
            database='bod_airflow', user='postgres', password='Post33Gres', host='127.0.0.1', port='5432'
        )
        # Setting auto commit false
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Could not connect to database: {str(e)}")

    try:
        petl.io.todb(exchange_rates, conn, 'canada_exchange_rates')
    except Exception as e:
        print(f"Could not write to database: {str(e)}")


bank_of_canada()