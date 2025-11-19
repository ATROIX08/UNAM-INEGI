import pandas as pd
import os

# 1. Definir las rutas de los archivos
# Usamos 'r' antes de las comillas para que Python interprete correctamente las barras invertidas de Windows
input_path = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\data\clean_Otras_Ingresos tributarios.xlsx"
output_dir = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\output"
output_file = os.path.join(output_dir, "impuestos_data_clean_trimestral.csv")

# 2. Cargar el dataset
print("Cargando archivo Excel...")
try:
    df = pd.read_excel(input_path)
except FileNotFoundError:
    print(f"Error: No se encontró el archivo en {input_path}")
    exit()

# 3. Limpieza y preparación de fechas
# Asegurarse de que la columna Fecha sea de tipo datetime
df['Fecha'] = pd.to_datetime(df['Fecha'])

# Establecer la fecha como el índice (necesario para re-muestrear por tiempo)
df.set_index('Fecha', inplace=True)

# 4. Seleccionar solo las columnas deseadas (ISR e IVA)
# Asegúrate de que los nombres coincidan exactamente con los de tu Excel
columnas_interes = ['Impuesto Sobre la Renta', 'Impuesto al Valor Agregado']
df_seleccion = df[columnas_interes]

# 5. Convertir de mensual a trimestral
# 'Q' significa Quarter (Trimestre).
# .sum() suma los meses del trimestre (Ej: Enero+Febrero+Marzo = Trimestre 1)
df_trimestral = df_seleccion.resample('Q').sum()

# Opcional: Si quieres renombrar las columnas para que sean más cortas en el CSV
df_trimestral.rename(columns={
    'Impuesto Sobre la Renta': 'ISR', 
    'Impuesto al Valor Agregado': 'IVA'
}, inplace=True)

# 6. Crear la carpeta de salida si no existe
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Carpeta creada: {output_dir}")

# 7. Guardar en CSV
print(f"Guardando archivo en: {output_file}")
df_trimestral.to_csv(output_file)

print("¡Proceso terminado con éxito!")