import pandas as pd
import os

# 1. Rutas
input_path = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\data\clean_Otras_ingreso obrero - patronal nacional 2005 - 2024.xlsx"
output_dir = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\output"
output_file = os.path.join(output_dir, "ingreso_obrero_patronal_trimestral.csv")

# 2. Cargar el dataset
print(f"Leyendo archivo...")
try:
    df = pd.read_excel(input_path)
except FileNotFoundError:
    print("¡Error! No se encuentra el archivo.")
    exit()

# --- CORRECCIÓN DEL ERROR ---
# Limpiamos los nombres de las columnas (quita espacios al inicio/final y saltos de línea)
df.columns = df.columns.str.strip()

# Imprimimos las columnas encontradas para verificar que 'Fecha' ya está limpia
print("Columnas detectadas:", df.columns.tolist())
# ----------------------------

# 3. Verificar si existe 'Fecha' después de limpiar
if 'Fecha' not in df.columns:
    # Si falla, intentamos renombrar la primera columna a 'Fecha' forzosamente
    print("Aviso: No se encontró la columna 'Fecha' exacta. Se usará la primera columna como Fecha.")
    df.rename(columns={df.columns[0]: 'Fecha'}, inplace=True)

# 4. Limpieza y formato de fecha
# dayfirst=True ayuda si el formato es dd/mm/yyyy
df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True)

# Establecemos la fecha como índice
df.set_index('Fecha', inplace=True)

# 5. Buscar la columna de Ingreso
# Como también limpiamos espacios, asegúrate de usar el nombre sin espacios extra
col_ingreso = 'Ingreso obrero - patronal nacional'

if col_ingreso not in df.columns:
    print(f"Error: No encuentro la columna '{col_ingreso}'.")
    print("Columnas disponibles:", df.columns.tolist())
    exit()

# 6. Convertir de mensual a trimestral (ACUMULADO)
df_trimestral = df[[col_ingreso]].resample('Q').sum()

# 7. Guardar
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

print(f"Guardando resultado en: {output_file}")
df_trimestral.to_csv(output_file)

print("¡Proceso completado con éxito!")