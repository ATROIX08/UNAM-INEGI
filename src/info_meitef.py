# -*- coding: utf-8 -*-
"""
Resumen estadístico del dataset MEITEF comercio informal (versión ligera, sin 'fecha').

- NO satura la terminal.
- Ignora completamente la columna 'fecha'.
- Muestra solo resúmenes y top N filas / categorías.
- Opcionalmente guarda tablas detalladas a CSV.

Estructura:
1) Carga del CSV.
2) Info general.
3) Conteo de valores únicos por columna (excepto valor).
4) Estadísticos descriptivos de columnas numéricas (sin 'fecha').
5) Resumen de columnas categóricas (top categorías).
6) Análisis metric vs indicador y guardado en CSV.
"""

import os
import pandas as pd

# ================== CONFIGURACIÓN ==================

FILE_PATH = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI\data\meitef_tidy\meitef_comercio_informal_tidy_ALL.csv"

# Máximo de filas / categorías a mostrar en la terminal
MAX_HEAD_ROWS = 5          # para df.head()
TOP_FREQ_CATEGORICAL = 5   # para value_counts de categóricas
TOP_METRIC_INDICADOR = 10  # para top pares metric-indicador

# Guardar tablas detalladas como CSV (True/False)
SAVE_DETAILED_TABLES = True
OUTPUT_DIR = "output_stats"

# ====================================================


def main():
    print("Cargando dataset...")
    df = pd.read_csv(FILE_PATH, encoding="latin-1")

    # ==== IMPORTANTE: ignorar por completo la columna 'fecha' ====
    if "fecha" in df.columns:
        df = df.drop(columns=["fecha"])
        print("\nColumna 'fecha' eliminada del análisis (no se mostrarán estadísticas de fechas).")

    print("\n===== Información general =====")
    print(f"Filas: {df.shape[0]:,}")
    print(f"Columnas: {df.shape[1]}")
    print("\nTipos de datos por columna:")
    print(df.dtypes)

    print(f"\nPrimeras {MAX_HEAD_ROWS} filas:")
    print(df.head(MAX_HEAD_ROWS))

    # 1. Definir columnas numéricas y categóricas
    # Ya no incluimos 'fecha' aquí
    potential_numeric = ["anio", "valor"]
    numeric_cols = [c for c in potential_numeric if c in df.columns]
    categorical_cols = [c for c in df.columns if c not in numeric_cols]

    print("\nColumnas numéricas detectadas:")
    print(numeric_cols)

    print("\nColumnas categóricas detectadas:")
    print(categorical_cols)

    # 2. Conteo de valores únicos por columna (excluyendo 'valor')
    print("\n===== Conteo de valores únicos por columna (excluyendo 'valor') =====")
    cols_para_unicos = [c for c in df.columns if c != "valor"]
    for col in cols_para_unicos:
        n_unique = df[col].nunique(dropna=True)
        print(f"- {col}: {n_unique} valores únicos")

    # 3. Estadísticos descriptivos de columnas numéricas
    if numeric_cols:
        print("\n===== Estadísticos descriptivos de columnas numéricas =====")
        desc = df[numeric_cols].describe(percentiles=[0.25, 0.5, 0.75])
        print(desc)

        print("\n===== Modas, medianas y cuartiles (resumen) =====")
        for col in numeric_cols:
            serie = df[col]
            print(f"\nColumna: {col}")
            print(f"  count: {serie.count()}")
            print(f"  media: {serie.mean()}")
            print(f"  mediana: {serie.median()}")
            print(f"  cuartil 25%: {serie.quantile(0.25)}")
            print(f"  cuartil 75%: {serie.quantile(0.75)}")
            modas = serie.mode(dropna=True)
            # solo mostramos las primeras 3 modas si hay muchas
            print(f"  primeras modas: {modas.head(3).tolist()}")
    else:
        print("\nNo se detectaron columnas numéricas.")

    # 4. Estadísticas de columnas categóricas (solo top categorías)
    print("\n===== Resumen de columnas categóricas (top categorías) =====")
    for col in categorical_cols:
        serie = df[col]
        n_unique = serie.nunique(dropna=True)
        print(f"\nColumna: {col}")
        print(f"  Valores únicos (sin NaN): {n_unique}")
        print(f"  Top {TOP_FREQ_CATEGORICAL} categorías (con conteos):")
        print(serie.value_counts(dropna=False).head(TOP_FREQ_CATEGORICAL))

    # 5. Análisis metric vs indicador (ligero)
    if "metric" in df.columns and "indicador" in df.columns:
        print("\n===== Análisis metric vs indicador =====")

        # Tabla de pares metric-indicador con número de observaciones
        tabla_pares = (
            df.groupby(["metric", "indicador"])
            .size()
            .reset_index(name="n_observaciones")
        )

        print("\nResumen general de combinaciones metric-indicador:")
        print(f"  Número total de combinaciones distintas: {tabla_pares.shape[0]:,}")
        print(f"  Top {TOP_METRIC_INDICADOR} combinaciones por número de observaciones:")
        print(
            tabla_pares.sort_values("n_observaciones", ascending=False)
            .head(TOP_METRIC_INDICADOR)
        )

        # Métricas que solo aparecen con un indicador
        metric_to_indicador_counts = (
            tabla_pares.assign(indicador_str=tabla_pares["indicador"].astype(str))
            .groupby("metric")["indicador_str"]
            .nunique()
            .reset_index(name="n_indicadores_distintos")
        )

        metrics_solo_un_indicador = metric_to_indicador_counts[
            metric_to_indicador_counts["n_indicadores_distintos"] == 1
        ]

        print("\nMétricas que solo aparecen con un indicador:")
        print(f"  Total de métricas en esta situación: {metrics_solo_un_indicador.shape[0]}")
        if not metrics_solo_un_indicador.empty:
            print("  Ejemplos (primeras 10):")
            print(metrics_solo_un_indicador.head(10))

        # Indicadores que solo aparecen con una métrica
        indicador_to_metric_counts = (
            tabla_pares.assign(metric_str=tabla_pares["metric"].astype(str))
            .groupby("indicador")["metric_str"]
            .nunique()
            .reset_index(name="n_metricas_distintas")
        )

        indicadores_solo_una_metrica = indicador_to_metric_counts[
            indicador_to_metric_counts["n_metricas_distintas"] == 1
        ]

        print("\nIndicadores que solo aparecen con una métrica:")
        print(
            f"  Total de indicadores en esta situación: {indicadores_solo_una_metrica.shape[0]}"
        )
        if not indicadores_solo_una_metrica.empty:
            print("  Ejemplos (primeros 10):")
            print(indicadores_solo_una_metrica.head(10))

        # 6. Guardar tablas detalladas a CSV (en lugar de imprimirlas)
        if SAVE_DETAILED_TABLES:
            os.makedirs(OUTPUT_DIR, exist_ok=True)

            tabla_pares.to_csv(
                os.path.join(OUTPUT_DIR, "metric_indicador_pairs.csv"),
                index=False,
                encoding="utf-8-sig",
            )

            metrics_solo_un_indicador.to_csv(
                os.path.join(OUTPUT_DIR, "metrics_solo_un_indicador.csv"),
                index=False,
                encoding="utf-8-sig",
            )

            indicadores_solo_una_metrica.to_csv(
                os.path.join(OUTPUT_DIR, "indicadores_solo_una_metrica.csv"),
                index=False,
                encoding="utf-8-sig",
            )

            print(
                f"\nTablas detalladas guardadas en la carpeta '{OUTPUT_DIR}/' "
                "(para abrir en Excel o donde quieras)."
            )
    else:
        print(
            "\nEl dataset no contiene columnas 'metric' e 'indicador', "
            "no se puede hacer el cruce."
        )

    print("\nFin del resumen estadístico (versión ligera, sin fechas).")


if __name__ == "__main__":
    main()
