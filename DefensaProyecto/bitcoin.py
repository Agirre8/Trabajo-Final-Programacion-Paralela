def bitcoinToEuros(bitcoin_amount, bitcoin_value_euros):
    euros_value = bitcoin_amount * bitcoin_value_euros
    return euros_value

bitcoin_amount = 0.5  # Cantidad de Bitcoin que posees
bitcoin_value_euros = 50000  # Valor de Bitcoin en euros

euros_value = bitcoinToEuros(bitcoin_amount, bitcoin_value_euros)

# Correlacionar con datos de tráfico aéreo
traffic_data = [10000, 12000, 15000, 18000, 20000]  # Datos de tráfico aéreo del aeropuerto de San Francisco
correlation = np.corrcoef([euros_value, *traffic_data])[0, 1]

# Comprobar si el valor cae por debajo de 30,000€
if euros_value < 30000:
    print("Alerta: El valor de tu Bitcoin ha caído por debajo de 30,000€.")

# Imprimir resultados
print("Valor de tu Bitcoin en euros:", euros_value)
print("Correlación entre el valor de Bitcoin y el tráfico aéreo:", correlation)
