import ccxt
import pandas as pd
import dask.dataframe as dd

def bitcoinToEuros(bitcoin_amount, bitcoin_value_euros):
    euros_value = bitcoin_amount * bitcoin_value_euros
    return euros_value

def main():
    # Obtener el valor actual de Bitcoin en euros utilizando la API de CCXT
    exchange = ccxt.binance()
    ticker = exchange.fetch_ticker('BTC/EUR')
    bitcoin_value_euros = ticker['last']

    # Cargar los datos de tráfico aéreo del aeropuerto de San Francisco
    df = pd.read_csv('DATOS/air_traffic_data_limpio.csv')

    # Convertir el DataFrame de pandas a un DataFrame de Dask para paralelizar el procesamiento
    df_dask = dd.from_pandas(df, npartitions=4)

    # Calcular el valor de tu Bitcoin en euros utilizando la función bitcoinToEuros
    bitcoin_amount = 0.5  # Cantidad de Bitcoin que posees
    euros_value = bitcoinToEuros(bitcoin_amount, bitcoin_value_euros)

    # Realizar la correlación entre el valor de Bitcoin en euros y los datos de tráfico aéreo
    correlation_matrix = df_dask.corr()

    # Imprimir la matriz de correlación
    print("Matriz de correlación:")
    print(correlation_matrix)

    # Verificar si el valor de Bitcoin en euros cae por debajo de 30,000€ y enviar una alerta
    if euros_value < 30000:
        print("ALERTA: El valor de tu Bitcoin ha caído por debajo de 30,000€")

