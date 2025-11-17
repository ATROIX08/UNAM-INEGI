from pathlib import Path
import pandas as pd

# ================== RUTA DEL ARCHIVO ORIGEN ======================
FILE = Path(
    r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\data\enoe\enoe_comercio_estado_sexo_salud.xlsx"
)

# ================== LECTURA BRUTA DEL EXCEL ======================
# Leemos TODO sin encabezados porque los primeros 2 renglones son "metadatos"
# (trimestre y sexo) y A1:B2 están vacíos.
raw = pd.read_excel(FILE, header=None)

# Estructura que asumo por lo que comentas:
# fila 0 -> textos de trimestre ("Segundo trimestre del 2025", etc.) en C:FH
# fila 1 -> sexo ("Hombre", "Mujer", ...) en C:FH
# filas 2 en adelante -> datos:
#   col A -> variable de salud (Con acceso / Sin acceso / No especificado)
#   col B -> estado
#   col C:FH -> valores numéricos

# ================== EXTRAER METADATOS DE COLUMNAS ======================
quarters = raw.iloc[0, 2:]   # textos de trimestre (C1:FH1)
sexes    = raw.iloc[1, 2:]   # sexo (C2:FH2)

# ================== CUERPO DE DATOS (FILAS 3 A 98) ====================
acceso_salud = raw.iloc[2:, 0].reset_index(drop=True)  # A3:A98
estados      = raw.iloc[2:, 1].reset_index(drop=True)  # B3:B98
data_vals    = raw.iloc[2:, 2:].reset_index(drop=True).copy()  # C3:FH98

# Armamos un MultiIndex para las columnas con (trimestre_texto, sexo)
data_vals.columns = pd.MultiIndex.from_arrays(
    [quarters.values, sexes.values],
    names=["trimestre_texto", "sexo"]
)

# ================== PASAR DE ANCHO A LARGO =============================
# Hacemos stack sobre los niveles de columnas (trimestre_texto, sexo)
tidy = (
    data_vals
    .stack(["trimestre_texto", "sexo"])
    .reset_index()
)

# 'level_0' es el índice de la fila original (0..n-1 después de reset_index)
tidy = tidy.rename(columns={"level_0": "row_id", 0: "valor"})

# Volvemos a pegar info de acceso_salud y estado a partir de row_id
meta = pd.DataFrame({
    "row_id": acceso_salud.index,
    "acceso_salud": acceso_salud,
    "estado": estados,
})

tidy = tidy.merge(meta, on="row_id", how="left")

# ================== PARSEAR TRIMESTRE Y AÑO ============================
# Ejemplo de texto: "Segundo trimestre del 2025"
map_trimestre = {
    "Primer": 1,
    "Segundo": 2,
    "Tercer": 3,
    "Cuarto": 4,
}

def extrae_anio(texto):
    # Tomamos la última palabra como año (asumo siempre es ".... 2025")
    return int(str(texto).split()[-1])

def extrae_trimestre(texto):
    # Tomamos la primera palabra ("Primer", "Segundo", etc.)
    palabra = str(texto).split()[0]
    return map_trimestre.get(palabra)

tidy["anio"] = tidy["trimestre_texto"].apply(extrae_anio)
tidy["trimestre"] = tidy["trimestre_texto"].apply(extrae_trimestre)

# ================== CREAR PERIODO Y FECHA ==============================
# Usamos PeriodIndex trimestral con cierre en diciembre (Q-DEC).
# SUPUESTO: el trimestre es estándar:
#   1 -> Ene-Mar, 2 -> Abr-Jun, 3 -> Jul-Sep, 4 -> Oct-Dic
tidy["periodo"] = pd.PeriodIndex(
    year=tidy["anio"],
    quarter=tidy["trimestre"],
    freq="Q-DEC"
)

# Convertimos el periodo a una fecha (último día del trimestre)
tidy["fecha_fin_trimestre"] = tidy["periodo"].dt.to_timestamp(how="end")

# ================== ORDENAR Y LIMPIAR COLUMNAS ========================
tidy = tidy[
    [
        "estado",
        "acceso_salud",
        "sexo",
        "anio",
        "trimestre",
        "trimestre_texto",
        "periodo",
        "fecha_fin_trimestre",
        "valor",
    ]
].sort_values(["estado", "acceso_salud", "anio", "trimestre", "sexo"])

# ================== GUARDAR RESULTADO =================================
out_excel = FILE.with_name(FILE.stem + "_tidy.xlsx")
out_csv   = FILE.with_name(FILE.stem + "_tidy.csv")

tidy.to_excel(out_excel, index=False)
tidy.to_csv(out_csv, index=False, encoding="utf-8-sig")

print("Listo.")
print(f"Archivo tidy (Excel): {out_excel}")
print(f"Archivo tidy (CSV):   {out_csv}")
 