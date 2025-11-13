# -*- coding: utf-8 -*-
"""
Limpieza MEITEF comercio informal (VAB, remuneraciones, puestos)
Formatea los excels en tablas tidy listas para usar con pandas.

Además:
- Para remuneraciones (MEITEF_79.xlsx) se generan valores a precios de 2018
  usando el "Índice de precios implícitos base 2018=100" que viene en MEITEF_14.

Requisitos:
    pip install pandas openpyxl
"""

from pathlib import Path
import re
import pandas as pd
from pandas.tseries.offsets import MonthEnd

# ==== RUTAS BÁSICAS (AJUSTA SI CAMBIAS CARPETAS) ============================
BASE_DIR = Path(
    r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\data"
)
RAW_DIR = BASE_DIR / "meitef_raw"
OUT_DIR = BASE_DIR / "meitef_tidy"   # nueva carpeta de salida dentro de data
OUT_DIR.mkdir(exist_ok=True)

# ==== PARÁMETROS DE ESTRUCTURA DEL EXCEL ====================================
# Fila 5 en Excel (años) -> índice 4 en pandas
ROW_YEAR = 4
# Fila 6 en Excel (T1, T2, T3, T4, 6 meses, 9 meses, Anual) -> índice 5
ROW_PERIOD = 5

# Número de renglones de estados (México + 32 entidades)
N_ESTADOS = 33


def periodo_to_month(periodo: str) -> int | None:
    """
    Mapea el texto del periodo ('T1', '6 Meses', 'Anual', etc.) al mes
    cuyo final representa ese periodo (3, 6, 9, 12).
    """
    if pd.isna(periodo):
        return None

    s = str(periodo).strip().lower()

    # Trimestres
    if s.startswith("t1") or "1er" in s:
        return 3
    if s.startswith("t2") or "2do" in s or "2º" in s:
        return 6
    if s.startswith("t3") or "3er" in s:
        return 9
    if s.startswith("t4") or "4to" in s or "4º" in s:
        return 12

    # Acumulados
    if "6" in s and "mes" in s:
        return 6
    if "9" in s and "mes" in s:
        return 9
    if "anual" in s or "año" in s:
        return 12

    return None


def clean_anio_value(anio):
    """
    Limpia el valor de año para casos como '2018R', '2019p', etc.
    Devuelve solo el entero de 4 dígitos si lo encuentra.
    """
    if pd.isna(anio):
        return pd.NA

    s = str(anio)
    m = re.search(r"\d{4}", s)
    if m:
        return int(m.group(0))
    return pd.NA


def procesar_meitef(path_xlsx: Path, indicador: str, title_rows_excel: list[int]) -> pd.DataFrame:
    """
    Lee un archivo MEITEF_xxx.xlsx y devuelve un DataFrame tidy con:

    columnas: [estado, anio, periodo, fecha, metric, indicador, fuente, valor]
    donde:
        - 'metric' es el nombre de la tabla (p.ej. 'Millones de pesos a precios de 2018')
        - 'indicador' es VAB / remuneraciones / puestos (lo pasas como argumento)
        - 'fecha' es el último día del mes correspondiente al periodo.
    """

    # Leemos TODO el archivo sin encabezados (para controlar los títulos a mano)
    df_raw = pd.read_excel(path_xlsx, header=None, engine="openpyxl")

    # Columnas útiles (desde B en adelante, donde están T1, T2, ... Anual)
    valid_cols = [
        c for c in df_raw.columns[1:]
        if pd.notna(df_raw.iloc[ROW_PERIOD, c])
    ]

    # Fila de años (con merges) y fila de trimestres / acumulados
    year_row = df_raw.iloc[ROW_YEAR, valid_cols].ffill()  # rellenamos merges hacia la derecha
    period_row = df_raw.iloc[ROW_PERIOD, valid_cols]

    # MultiIndex columnas (anio, periodo) para poder hacer un stack elegante
    cols_multi = pd.MultiIndex.from_arrays(
        [year_row.to_numpy(), period_row.to_numpy()],
        names=["anio", "periodo"],
    )

    registros = []

    for title_row_excel in title_rows_excel:
        # title_row_excel: fila de Excel donde está el título de la tabla (1-based)
        title_idx = title_row_excel - 1      # a índice 0-based
        start_idx = title_row_excel          # primera fila de estados (1 fila abajo del título)

        metric_name = str(df_raw.iloc[title_idx, 0]).strip()

        # Bloque de estados para esa tabla (México + 32 entidades)
        sub = df_raw.iloc[start_idx:start_idx + N_ESTADOS, :]

        # Si por alguna razón está vacío, lo brincamos
        if sub.iloc[:, 0].isna().all():
            continue

        # Nombre de los estados (columna A)
        estados = (
            sub.iloc[:, 0]
               .astype(str)
               .str.strip()
               .replace({"": pd.NA})
        )

        # Valores numéricos (columnas B en adelante)
        sub_vals = sub.loc[:, valid_cols].copy()
        sub_vals.columns = cols_multi

        # Pasamos de wide (muchas columnas de años/trimestres) a long/tidy
        long = (
            sub_vals
              .stack(level=[0, 1], future_stack=True)  # nueva implementación de stack
              .reset_index()
        )

        long = long.rename(columns={"level_0": "row_id", 0: "valor"})

        # row_id es el índice de fila original; lo pasamos a 0..32 para mapear estados
        long["row_id"] = long["row_id"] - start_idx
        long["estado"] = estados.iloc[long["row_id"]].values
        long.drop(columns=["row_id"], inplace=True)

        # Metadatos
        long["metric"] = metric_name                       # nombre de la tabla
        long["indicador"] = indicador                      # VAB / remuneraciones / puestos
        long["fuente"] = path_xlsx.name                    # nombre del archivo

        registros.append(long)

    # Unimos todas las tablas de ese archivo
    df = pd.concat(registros, ignore_index=True)

    # Limpieza y tipos
    df["anio"] = df["anio"].apply(clean_anio_value).astype("Int64")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # Quitamos filas sin estado o sin año (ya limpio)
    df = df.dropna(subset=["estado", "anio"])

    # ====== CÁLCULO DE LA FECHA (FIN DE MES) ================================
    df["periodo_str"] = df["periodo"].astype(str)
    df["mes"] = df["periodo_str"].apply(periodo_to_month)

    df["fecha"] = pd.to_datetime(
        {
            "year": df["anio"].astype("int64"),
            "month": df["mes"].astype("int64"),
            "day": 1,
        },
        errors="coerce",
    ) + MonthEnd(0)

    # Orden de columnas base
    df = df[["estado", "anio", "periodo", "fecha", "metric", "indicador", "fuente", "valor"]]

    return df


def deflactar_remuneraciones_con_ipi(
    df_rem: pd.DataFrame,
    df_ipi_raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Usa el Índice de precios implícitos base 2018=100 del VAB (MEITEF_14)
    para llevar las remuneraciones de MEITEF_79 (corrientes) a precios de 2018.

    Fórmula:
        remu_const_2018 = remu_corriente * 100 / indice_precios

    df_rem: tidy de MEITEF_79 (remuneraciones)
    df_ipi_raw: tidy filtrado a "Índice de precios implícitos base 2018=100"
                proveniente de MEITEF_14.
    """

    # 1. Filas de remuneraciones corrientes
    mask_nom = df_rem["metric"].str.contains(
        "millones de pesos a precios corrientes",
        case=False,
        na=False,
    )

    df_nom = df_rem.loc[mask_nom].copy()

    if df_nom.empty:
        print("ADVERTENCIA: no encontré 'Millones de pesos a precios corrientes' en remuneraciones.")
        return df_rem

    df_nom = df_nom[
        ["estado", "anio", "periodo", "fecha", "indicador", "fuente", "valor"]
    ].rename(columns={"valor": "remu_corriente"})

    # 2. Preparar el índice de precios implícitos
    df_ipi = df_ipi_raw.copy()
    df_ipi = df_ipi[
        ["estado", "anio", "periodo", "valor"]
    ].rename(columns={"valor": "indice_precios"})

    # IMPORTANTE: quitar duplicados en el deflactor para evitar MergeError
    df_ipi = (
        df_ipi
        .dropna(subset=["estado", "anio", "periodo"])
        .sort_values(["estado", "anio", "periodo"])
        .drop_duplicates(subset=["estado", "anio", "periodo"], keep="first")
    )

    # 3. Merge por estado-anio-periodo
    merge_cols = ["estado", "anio", "periodo"]

    df_merged = df_nom.merge(
        df_ipi,
        on=merge_cols,
        how="inner",          # sólo combinaciones donde hay deflactor
        validate="many_to_one"  # varias remuneraciones podrían mapear a un solo deflactor
    )

    # 4. Limpiar NAs y ceros
    df_merged = df_merged.dropna(subset=["remu_corriente", "indice_precios"])
    df_merged = df_merged[df_merged["indice_precios"] != 0]

    # 5. Cálculo a precios constantes de 2018
    df_merged["valor"] = df_merged["remu_corriente"] * 100.0 / df_merged["indice_precios"]

    df_merged["metric"] = (
        "Remuneraciones a precios de 2018 "
        "(deflactadas con Índice de precios implícitos del VAB)"
    )

    df_def = df_merged[
        ["estado", "anio", "periodo", "fecha", "metric", "indicador", "fuente", "valor"]
    ]

    # 6. Unimos con el df original de remuneraciones
    df_out = pd.concat([df_rem, df_def], ignore_index=True)

    return df_out


# ==== CONFIGURACIÓN DE LOS 3 ARCHIVOS =======================================

files_info = [
    {
        "filename": "MEITEF_14.xlsx",
        "indicador": "vab_comercio_informal",
        # Títulos de cada tabla (filas de Excel: 1-based)
        # 7  = Millones de pesos a precios de 2018
        # 41 = Participación porcentual (constantes)
        # 75 = Índice volumen físico base 2018=100
        # 109 = Variación porcentual anual del índice de volumen
        # 143 = Contribución a la variación nacional (volumen)
        # 177 = Millones de pesos a precios corrientes
        # 211 = Participación porcentual en valores corrientes
        # 245 = Índice de precios implícitos base 2018=100
        # 279 = Variación porcentual del índice de precios implícitos
        "title_rows_excel": [7, 41, 75, 109, 143, 177, 211, 245, 279],
    },
    {
        "filename": "MEITEF_79.xlsx",
        "indicador": "remuneraciones_comercio_informal",
        # En este archivo sólo existen las primeras 5 tablas:
        # 7   = Millones de pesos a precios corrientes
        # 41  = Participación porcentual
        # 75  = Índice de valor 2018=100
        # 109 = Variación porcentual anual del índice de valor
        # 143 = Contribución a la variación nacional del valor
        "title_rows_excel": [7, 41, 75, 109, 143],
    },
    {
        "filename": "MEITEF_144.xlsx",
        "indicador": "puestos_trabajo_comercio_informal",
        # Igual: primeras 5 tablas
        "title_rows_excel": [7, 41, 75, 109, 143],
    },
]

# ==== EJECUCIÓN: GENERAR CSVs LIMPIOS =======================================

all_df = []
ipi_deflator_df: pd.DataFrame | None = None  # aquí guardaremos el índice de precios implícitos

for info in files_info:
    path_xlsx = RAW_DIR / info["filename"]
    print(f"Procesando {path_xlsx} ...")

    df_tidy = procesar_meitef(
        path_xlsx=path_xlsx,
        indicador=info["indicador"],
        title_rows_excel=info["title_rows_excel"],
    )

    # Si es el VAB, extraemos el Índice de precios implícitos como deflactor
    if info["filename"] == "MEITEF_14.xlsx":
        mask_ipi = df_tidy["metric"].str.contains(
            "índice de precios implícitos",
            case=False,
            na=False,
        ) | df_tidy["metric"].str.contains(
            "indice de precios implicitos",
            case=False,
            na=False,
        )
        ipi_deflator_df = df_tidy.loc[mask_ipi].copy()
        if ipi_deflator_df.empty:
            print("ADVERTENCIA: no encontré 'Índice de precios implícitos base 2018=100' en MEITEF_14.")

    # Para remuneraciones, deflactamos con ese índice de precios
    if info["indicador"] == "remuneraciones_comercio_informal":
        if ipi_deflator_df is None:
            print("ADVERTENCIA: no hay deflactor (IPI) disponible; remuneraciones se quedan solo en corrientes.")
        else:
            df_tidy = deflactar_remuneraciones_con_ipi(df_tidy, ipi_deflator_df)

    # Rellenamos NAs en valor con 0 (después de todo el procesamiento)
    df_tidy["valor"] = df_tidy["valor"].fillna(0)

    all_df.append(df_tidy)

    # Guardamos un CSV por indicador
    out_file = OUT_DIR / f"{info['indicador']}_tidy.csv"
    df_tidy.to_csv(out_file, index=False, encoding="utf-8-sig")
    print(f"  -> Guardado: {out_file}")

# Dataset maestro con todo junto
df_total = pd.concat(all_df, ignore_index=True)
out_master = OUT_DIR / "meitef_comercio_informal_tidy_ALL.csv"
df_total.to_csv(out_master, index=False, encoding="utf-8-sig")
print(f"\nArchivo maestro guardado en: {out_master}")
