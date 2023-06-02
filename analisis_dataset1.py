import pickle

#cargar la lista desde el archivo
with open("DATOS/lista.pkl", "rb") as archivo:
    valor_dict = pickle.load(archivo)
    mapping_dict_Boarding_Area = pickle.load(archivo)
    mapping_dict_Geo_Region = pickle.load(archivo)
    mapping_dict_Geo_Summary = pickle.load(archivo)
    mapping_dict_Month = pickle.load(archivo)
    mapping_dict_Price_Category_Code = pickle.load(archivo)
    mapping_dict_Terminal = pickle.load(archivo)

#invierto el diccionario para poder llamar mas facil a las claves posteriormente
valor_dict_invertido = {valor: clave for clave, valor in valor_dict.items()}
mapping_dict_Boarding_Area_invertido = {valor: clave for clave, valor in mapping_dict_Boarding_Area.items()}
mapping_dict_Geo_Region_invertido = {valor: clave for clave, valor in mapping_dict_Geo_Region.items()}
mapping_dict_Geo_Summary_invertido = {valor: clave for clave, valor in mapping_dict_Geo_Summary.items()}
mapping_dict_Month_invertido = {valor: clave for clave, valor in mapping_dict_Month.items()}
mapping_dict_Price_Category_Code_invertido = {valor: clave for clave, valor in mapping_dict_Price_Category_Code.items()}
mapping_dict_Terminal_invertido = {valor: clave for clave, valor in mapping_dict_Terminal.items()}

# Imprimir los diccionarios
print("valor_dict:", valor_dict)
print("valor_dict_invertido:", valor_dict_invertido)
print("mapping_dict_Boarding_Area:", mapping_dict_Boarding_Area)
print("mapping_dict_Boarding_Area_invertido:", mapping_dict_Boarding_Area_invertido)
print("mapping_dict_Geo_Region:", mapping_dict_Geo_Region)
print("mapping_dict_Geo_Region_invertido:", mapping_dict_Geo_Region_invertido)
print("mapping_dict_Geo_Summary:", mapping_dict_Geo_Summary)
print("mapping_dict_Geo_Summary_invertido:", mapping_dict_Geo_Summary_invertido)
print("mapping_dict_Month:", mapping_dict_Month)
print("mapping_dict_Month_invertido:", mapping_dict_Month_invertido)
print("mapping_dict_Price_Category_Code:", mapping_dict_Price_Category_Code)
print("mapping_dict_Price_Category_Code_invertido:", mapping_dict_Price_Category_Code_invertido)
print("mapping_dict_Terminal:", mapping_dict_Terminal)
print("mapping_dict_Terminal_invertido:", mapping_dict_Terminal_invertido)




from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

#crear una instancia de SparkSession
spark = SparkSession.builder.getOrCreate()

#cargo el nuevo csv limpio con pyspark, luego lo voy a hacer tambien con Dask
df = spark.read.csv("DATOS/air_traffic_data_limpio.csv", header=True, inferSchema=True)

#media de pasajeros con pysparc
media_pasajeros = df.groupBy("Operating Airline").agg(avg("Passenger Count").alias("MediaPasajeros"))

#muestro el resultado, truncate=False para que no solo me salgan los 20 primeros
media_pasajeros.show(truncate=False)



from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, col

spark = SparkSession.builder.getOrCreate()

# Calculate the mean and standard deviation of passengers by airline
media_pasajeros = df.groupBy("Operating Airline").agg(avg("Passenger Count").alias("Media_Pasajeros"))
desviacion_pasajeros = df.groupBy("Operating Airline").agg(stddev("Passenger Count").alias("Desviacion_Pasajeros"))

# Find the smallest mean and standard deviation values
menor_valor_media = media_pasajeros.selectExpr("MIN(Media_Pasajeros)").first()[0]
menor_valor_desviacion = desviacion_pasajeros.selectExpr("MIN(Desviacion_Pasajeros)").first()[0]

# Filter the DataFrame to get the record with the smallest MediaPasajeros
menor_registro_media = media_pasajeros.filter(col("Media_Pasajeros") == menor_valor_media).first()
aerolinea_menor_media = menor_registro_media["Operating Airline"]

# Filter the DataFrame to get the record with the smallest DesviacionPasajeros
menor_registro_desviacion = desviacion_pasajeros.filter(col("Desviacion_Pasajeros") == menor_valor_desviacion).first()
aerolinea_menor_desviacion = menor_registro_desviacion["Operating Airline"]

# Find the largest mean and standard deviation values
mayor_valor_media = media_pasajeros.selectExpr("MAX(Media_Pasajeros)").first()[0]
mayor_valor_desviacion = desviacion_pasajeros.selectExpr("MAX(Desviacion_Pasajeros)").first()[0]

# Filter the DataFrame to get the record with the largest MediaPasajeros
mayor_registro_media = media_pasajeros.filter(col("Media_Pasajeros") == mayor_valor_media).first()
aerolinea_mayor_media = mayor_registro_media["Operating Airline"]

# Filter the DataFrame to get the record with the largest DesviacionPasajeros
mayor_registro_desviacion = desviacion_pasajeros.filter(col("Desviacion_Pasajeros") == mayor_valor_desviacion).first()
aerolinea_mayor_desviacion = mayor_registro_desviacion["Operating Airline"]

print("Media de Pasajeros más Pequeña:", menor_valor_media, "(Aerolínea correspondiente:", valor_dict_invertido[aerolinea_menor_media], ")")
print("Desviación Estándar más Pequeña:", menor_valor_desviacion, "(Aerolínea correspondiente:", valor_dict_invertido[aerolinea_menor_desviacion], ")")
print("Media de Pasajeros más Grande:", mayor_valor_media, "(Aerolínea correspondiente:", valor_dict_invertido[aerolinea_mayor_media], ")")
print("Desviación Estándar más Grande:", mayor_valor_desviacion, "(Aerolínea correspondiente:", valor_dict_invertido[aerolinea_mayor_desviacion], ")")



#CONCLUSIONES:
#compruebo cuantos vuelos han salido de Xtra Airways ya que la desviacion estandar es 0 y eso es bastante extraño
import dask.dataframe as dd

df = dd.read_csv("DATOS/air_traffic_data_limpio.csv")

cantidad_operating_airlines = (df["Operating Airline"] == valor_dict["Xtra Airways"]).sum().compute()

print(f"Hay {cantidad_operating_airlines} vuelos registrados de la compañía Xtra Airways, por eso mismo la desviación estándar es de 0, porque los dos vuelos tienen la misma cantidad de pasajeros")

#así mismo, si compruebo la cantidad de vuelos de American Airlines seguro que es más grande ya que tiene una desviación estándar elevada

cantidad_operating_airlines = (df["Operating Airline"] == valor_dict["United Airlines"]).sum().compute()

print(f"Hay {cantidad_operating_airlines} vuelos registrados de la compañía United Airlines, tiene sentido que haya tantos ya que tiene una alta desviación estándar")



#Compruebo el precio medio de los 2 vuelos de XtraAirways y veo que son de 1.0, es decir muy caro, tiene sentido que haya tantos vuelos
media_precio_compania = df[df["Operating Airline"] == valor_dict["Xtra Airways"]]["Price Category Code"].mean().compute()

print(f"Precio medio de los vuelos de Xtra Airways: {media_precio_compania} que es: {mapping_dict_Price_Category_Code_invertido[media_precio_compania]}")




import dask.dataframe as dd
import seaborn as sns
import matplotlib.pyplot as plt

# Calcular la matriz de correlación y computarla
matriz_correlacion = df.corr().compute()

# Crear una figura y un eje
fig, ax = plt.subplots(figsize=(10, 8))

# Generar el mapa de calor de la matriz de correlación
sns.heatmap(matriz_correlacion, annot=True, cmap="coolwarm", ax=ax)

# Guardar la figura como imagen PNG
plt.savefig("Graficos/Matriz_Correlacion.png")



print(matriz_correlacion)





print("\n-----CONCLUSIONES DE CORRELACION-----")

print("\nActivity Period y Year tienen una correlación positiva muy alta de 0.999940 que indica una fuerte relación entre estos dos atributos. Esto sugiere que el período de actividad y el año están estrechamente relacionados en los datos.")

print("\nOperating Airline y GEO Summary tienen una correlación negativa moderada de -0.067780. Esto indica que hay una relación inversa entre la aerolínea operativa y el resumen geográfico. Es decir, ciertos resúmenes geográficos pueden estar asociados con aerolíneas específicas.")

print("\nGEO Summary y GEO Region tienen una correlación positiva alta de 0.756758. Esto sugiere que el resumen geográfico y la región geográfica están relacionados de manera significativa. Es decir, ciertos resúmenes geográficos están asociados con regiones específicas.")

print("\nPassenger Count y GEO Summary tienen una correlación negativa moderada de -0.395743. Esto indica que ciertos resúmenes geográficos pueden tener una influencia en la cantidad de pasajeros")




import matplotlib.pyplot as plt

importance = model.feature_importances_

# Crear un dataframe con la importancia de cada característica
importance_df = pd.DataFrame({'Feature': X_train.columns, 'Importance': importance})

# Ordenar el dataframe por importancia descendente
importance_df = importance_df.sort_values(by='Importance', ascending=False)

# Imprimir los resultados
print(importance_df)


# Plotear el gráfico de barras de las importancias
plt.figure(figsize=(10, 6))
plt.bar(importance_df['Feature'], importance_df['Importance'])
plt.xticks(rotation=90)
plt.xlabel('Característica')
plt.ylabel('Importancia')
plt.title('Importancia de las características en el modelo de árbol de decisiones')
plt.tight_layout()

# Guardar el gráfico como un archivo PNG
plt.savefig('importance_plot.png')

# Mostrar el gráfico en la ventana
plt.show()





# Calculate the mean absolute error (MAE)
mae = mean_absolute_error(y_test, y_pred)

# Plot the predicted values vs. the true values
plt.scatter(y_test, y_pred)
plt.xlabel("True Values")
plt.ylabel("Predicted Values")
plt.title("True vs. Predicted Values")
plt.savefig("Graficos/True_vs_Predicted_Values.png")
plt.show()

# Print the mean absolute error (MAE)
print("Mean Absolute Error:", mae)





from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import GridSearchCV

# Definir rango de profundidades
depths = range(2, 31)

# Crear modelo Decision Tree Classifier
dtc = DecisionTreeClassifier(random_state=100)

# Definir parámetros de GridSearchCV
param_grid = {"max_depth": depths}

# Realizar GridSearchCV
grid_search = GridSearchCV(dtc, param_grid, cv=5, scoring="accuracy")
grid_search.fit(X_train, y_train)

# Guardar resultados de GridSearchCV
results = grid_search.cv_results_

# Obtener los valores de los parámetros y el score
params = results["params"]
scores = results["mean_test_score"]

# Crear gráfica de curva de complejidad
plt.plot(depths, scores, "-o")
plt.xlabel("Profundidad")
plt.ylabel("Accuracy")
plt.title("Curva de complejidad del modelo Decision Tree")
plt.xticks(depths)

plt.savefig("Graficos/Curva de complejidad de arbol de decision.png")

plt.show()

# Obtener mejor valor de profundidad del árbol
best_depth = grid_search.best_params_["max_depth"]
print("El valor óptimo de la profundidad del árbol es:", best_depth)





import pandas as pd
import dask.dataframe as dd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

# Convert Dask DataFrame to Pandas DataFrame
df_pandas = df.compute()  

# Dividir los datos en características (X) y etiquetas (y)
X = df_pandas.drop('Price Category Code', axis=1)
y = df_pandas['Price Category Code']

# Dividir los datos en conjuntos de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=100)

# Crear el modelo de bosque de decisiones
model = RandomForestRegressor(max_depth=24, random_state=100)

# Entrenar el modelo
model.fit(X_train, y_train)

# Realizar predicciones en el conjunto de prueba
y_pred = model.predict(X_test)

# Calcular el error absoluto medio
mae = mean_absolute_error(y_test, y_pred)

# Imprimir el error absoluto medio
print('Error absoluto medio:', mae)




print("\n-----CONCLUSIONES ENTRE ARBOL Y BOSQUE DE DECISION-----")

print("Haciendo unRandom Forest de profundidad 24 (la óptima) cometo mas error las siguientes razones:\n ")
print("Cuando los datos son simples, un árbol de decisión individual puede ser suficiente para capturar las relaciones \nlineales o patrones directos en los datos. Un Random Forest, al ser una combinación de múltiples árboles, puede agregar cierta complejidad \nadicional y, en algunos casos, conducir a un rendimiento similar o ligeramente peor en comparación con un árbol de decisión individual.")

