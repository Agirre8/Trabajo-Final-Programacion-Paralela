import pandas as pd
import matplotlib.pyplot as plt

# Datos de la tabla
tabla = {
    'Nombre': ['Juan', 'María', 'Carlos'],
    'Edad': [25, 30, 35],
    'Ciudad': ['México', 'Madrid', 'Buenos Aires']
}

# Crear un DataFrame con los datos de la tabla
df = pd.DataFrame(tabla)

# Configurar el estilo de la tabla
estilo = df.style \
    .set_properties(**{'text-align': 'center'}) \
    .bar(subset=['Edad'], color='#5fba7d') \
    .background_gradient(subset=['Nombre'], cmap='Blues')

# Mostrar la tabla
fig, ax = plt.subplots(figsize=(6, 4))
ax.axis('off')
ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', colLoc='center', cellColours=None, loc='center', edges='B', cellLocKw={'style': 'bold'}, bbox=[0, 0, 1, 1])
plt.title('Tabla de datos', fontweight='bold')
plt.show()