# -*- coding: utf-8 -*-
"""
Generador de gráficas para el análisis de la economía informal con MEITEF
Dataset: meitef_comercio_informal_tidy_ALL.csv

Este script:
- Carga el dataset MEITEF (comercio informal).
- Limpia mínimamente los nombres de columnas (estado) y construye una fecha trimestral.
- RESTRINGE el análisis hasta el 4T de 2024 (fecha <= 31/12/2024).
- Genera visualizaciones formales:
    * Series de tiempo nacionales por indicador y métrica.
    * Series de tiempo por estado.
    * Comparaciones nacional vs principales estados (trimestral).
    * Rankings y participaciones estatales (año de referencia: T4).
    * Heatmap estado–año.
    * Series de variación porcentual anual.

Todas las gráficas se guardan en subcarpetas dentro de:
    .../UNAM-INEGI/output/plots/*
"""

from pathlib import Path
import re
import unicodedata

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter


# ---------------------------------------------------------------------------
# Configuración básica
# ---------------------------------------------------------------------------

BASE_DIR = Path(
    r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI"
)
DATA_PATH = BASE_DIR / "data" / "meitef_tidy" / "meitef_comercio_informal_tidy_ALL.csv"
OUTPUT_PLOTS_DIR = BASE_DIR / "output" / "plots"


def configurar_estilo():
    """Configura un estilo sobrio y formal para todas las gráficas."""
    sns.set_theme(style="whitegrid")
    plt.rcParams["figure.figsize"] = (10, 6)
    plt.rcParams["axes.titlesize"] = 14
    plt.rcParams["axes.labelsize"] = 12
    plt.rcParams["xtick.labelsize"] = 10
    plt.rcParams["ytick.labelsize"] = 10
    plt.rcParams["legend.fontsize"] = 10
    plt.rcParams["figure.dpi"] = 120
    # Evitar notación científica “1e5”
    plt.rcParams["axes.formatter.useoffset"] = False
    plt.rcParams["axes.formatter.limits"] = (-9, 9)


def asegurar_directorio(path: Path):
    """Crea el directorio si no existe."""
    path.mkdir(parents=True, exist_ok=True)


def slugify(text: str) -> str:
    """
    Convierte un texto arbitrario en un 'slug' apto para nombre de archivo:
    - Quita acentos.
    - Reemplaza cualquier cosa no alfanumérica por '_'.
    - Pasa a minúsculas.
    """
    if text is None:
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text)
    return text.strip("_").lower()


# ---------------------------------------------------------------------------
# Formateadores para ejes
# ---------------------------------------------------------------------------

def fmt_monetario_millones(x, pos):
    """Formatea montos que ya están en millones de pesos."""
    return "$" + format(x, ",.0f")


def fmt_cantidad(x, pos):
    """Formatea cantidades (unidades, índices) con separador de miles."""
    return format(x, ",.0f")


def fmt_porcentaje(x, pos):
    """Formatea porcentajes."""
    return f"{x:.1f}%"


def aplicar_formato_eje_y_metric(ax, metric: str):
    """
    Aplica formato de eje Y en función del texto de 'metric':
      - 'millones de pesos' -> monetario
      - 'unidades' -> cantidad
      - 'índice' -> cantidad
    """
    m = metric.lower()
    if "millones de pesos" in m:
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_monetario_millones))
    elif "unidades" in m:
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_cantidad))
    elif "indice" in m or "índice" in m or "ndice" in m:
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_cantidad))
    # para otros casos dejamos el default (sin notación científica por rcParams)


# ---------------------------------------------------------------------------
# Carga y limpieza mínima
# ---------------------------------------------------------------------------

def construir_fecha_desde_anio_periodo(df: pd.DataFrame) -> pd.Series:
    """
    Construye una fecha aproximada a partir de (anio, periodo).
    Se interpreta que los datos son trimestrales o acumulados:
        T1 -> marzo
        T2 -> junio
        T3 -> septiembre
        T4 -> diciembre
        6 Meses -> junio
        9 Meses -> septiembre
        12 Meses / Año / Anual -> diciembre
    """
    def periodo_a_mes(p):
        p = str(p).strip()
        if p == "T1":
            return 3
        if p == "T2":
            return 6
        if p == "T3":
            return 9
        if p == "T4":
            return 12
        if "6 Meses" in p:
            return 6
        if "9 Meses" in p:
            return 9
        if "12 Meses" in p or "Año" in p or "Anual" in p or "Anio" in p:
            return 12
        return 12  # fallback: diciembre

    meses = df["periodo"].map(periodo_a_mes)
    fechas = pd.to_datetime(
        dict(year=df["anio"].astype(int), month=meses.astype(int), day=1)
    )
    return fechas


def cargar_y_preparar_datos(path: Path) -> pd.DataFrame:
    """Carga el CSV MEITEF y realiza limpieza mínima necesaria para graficar."""
    print("Cargando dataset MEITEF desde:", path)
    df = pd.read_csv(path)

    # Renombrar columna de estado si viene con BOM
    if "ï»¿estado" in df.columns:
        df = df.rename(columns={"ï»¿estado": "estado"})

    columnas_esperadas = {"estado", "anio", "periodo", "metric", "indicador", "fuente", "valor"}
    faltantes = columnas_esperadas.difference(df.columns)
    if faltantes:
        raise ValueError(f"Hacen falta columnas esperadas en el dataset: {faltantes}")

    # Construir fecha si no existe o si viene en formato Excel numérico
    if "fecha" in df.columns:
        if np.issubdtype(df["fecha"].dtype, np.number):
            df["fecha"] = pd.to_datetime(df["fecha"], origin="1899-12-30", unit="D")
        else:
            df["fecha"] = pd.to_datetime(df["fecha"])
    else:
        df["fecha"] = construir_fecha_desde_anio_periodo(df)

    # FILTRO TEMPORAL: solo hasta el 4T de 2024
    corte = pd.Timestamp(2024, 12, 31)
    df = df[df["fecha"] <= corte].copy()

    # Orden general
    df = df.sort_values(["estado", "indicador", "metric", "fecha"]).reset_index(drop=True)

    # Bandera nacional vs estatales
    df["es_nacional"] = df["estado"].eq("Estados Unidos Mexicanos")

    print("Filas totales (hasta 4T-2024):", len(df))
    print("Rango temporal:", df["fecha"].min().date(), "→", df["fecha"].max().date())
    print("Estados (incluye nacional):", df["estado"].nunique())

    return df


# ---------------------------------------------------------------------------
# Utilidades de filtrado
# ---------------------------------------------------------------------------

def filtrar_subset(df: pd.DataFrame, indicador: str, metric: str) -> pd.DataFrame:
    """Filtra el DataFrame por indicador y métrica exactos."""
    sub = df[(df["indicador"] == indicador) & (df["metric"] == metric)].copy()
    return sub


def elegir_periodo_referencia(sub: pd.DataFrame) -> str:
    """
    Elige un periodo de referencia para análisis anual.
    Dado que los datos son trimestrales, privilegiamos T4:
      1) 'T4'
      2) '12 Meses'
      3) 'Año' / 'Anual' / 'Anio'
      4) periodo más frecuente
    """
    periodos = sub["periodo"].dropna().unique().tolist()
    if not periodos:
        raise ValueError("No hay información en 'periodo' para este subconjunto.")

    if "T4" in periodos:
        return "T4"
    if "12 Meses" in periodos:
        return "12 Meses"
    for p in ["Año", "Anual", "Anio"]:
        if p in periodos:
            return p

    modo = sub["periodo"].mode()
    return modo.iloc[0] if not modo.empty else periodos[0]


# ---------------------------------------------------------------------------
# 1. Series de tiempo nacionales (niveles e índices)
# ---------------------------------------------------------------------------

def generar_series_nacionales(df: pd.DataFrame, output_dir: Path):
    asegurar_directorio(output_dir)

    combos_nivel = [
        ("vab_comercio_informal", "Millones de pesos a precios corrientes"),
        ("vab_comercio_informal", "Millones de pesos a precios de 2018"),
        ("remuneraciones_comercio_informal", "Millones de pesos a precios corrientes"),
        ("puestos_trabajo_comercio_informal", "Unidades"),
    ]

    combos_indice = [
        ("vab_comercio_informal", "Ãndice de volumen fÃ­sico base 2018=100"),
        ("remuneraciones_comercio_informal", "Ãndice de volumen fÃ­sico base 2018=100"),
        ("puestos_trabajo_comercio_informal", "Ãndice de volumen fÃ­sico base 2018=100"),
    ]

    # Series de niveles
    for indicador, metric in combos_nivel:
        sub = filtrar_subset(df, indicador, metric)
        sub = sub[sub["es_nacional"]]
        if sub.empty:
            continue

        sub = sub.sort_values("fecha")

        fig, ax = plt.subplots()
        ax.plot(sub["fecha"], sub["valor"], marker="o", linewidth=1.8)

        ax.set_title(
            f"Serie nacional – {indicador.replace('_', ' ')} ({metric})"
        )
        ax.set_xlabel("Fecha (trimestres)")
        ax.set_ylabel(metric)
        aplicar_formato_eje_y_metric(ax, metric)
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()

        fname = f"serie_nacional_{slugify(indicador)}_{slugify(metric)}.png"
        fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
        plt.close(fig)

    # Series de índices de volumen
    for indicador, metric in combos_indice:
        sub = filtrar_subset(df, indicador, metric)
        sub = sub[sub["es_nacional"]]
        if sub.empty:
            continue

        sub = sub.sort_values("fecha")

        fig, ax = plt.subplots()
        ax.plot(sub["fecha"], sub["valor"], marker="o", linewidth=1.8)

        ax.axhline(100, color="grey", linestyle="--", linewidth=1)
        ax.set_title(
            f"Índice de volumen – Serie nacional – {indicador.replace('_', ' ')}"
        )
        ax.set_xlabel("Fecha (trimestres)")
        ax.set_ylabel("Índice (2018 = 100)")
        aplicar_formato_eje_y_metric(ax, metric)
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()

        fname = f"indice_volumen_nacional_{slugify(indicador)}.png"
        fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
        plt.close(fig)


# ---------------------------------------------------------------------------
# 2. Series de tiempo por estado
# ---------------------------------------------------------------------------

def generar_series_estatales(df: pd.DataFrame, output_dir: Path):
    asegurar_directorio(output_dir)

    combos = [
        ("vab_comercio_informal", "Millones de pesos a precios de 2018"),  # VAB real
        ("remuneraciones_comercio_informal", "Millones de pesos a precios de 2018"),
        ("puestos_trabajo_comercio_informal", "Unidades"),
    ]

    estados = sorted(df.loc[~df["es_nacional"], "estado"].unique())

    for indicador, metric in combos:
        sub = filtrar_subset(df, indicador, metric)
        sub = sub[~sub["es_nacional"]]
        if sub.empty:
            continue

        for estado in estados:
            sub_e = sub[sub["estado"] == estado]
            if sub_e.empty:
                continue

            sub_e = sub_e.sort_values("fecha")

            fig, ax = plt.subplots()
            ax.plot(sub_e["fecha"], sub_e["valor"], marker="o", linewidth=1.4)

            ax.set_title(f"{estado} – {indicador.replace('_', ' ')} ({metric})")
            ax.set_xlabel("Fecha (trimestres)")
            ax.set_ylabel(metric)
            aplicar_formato_eje_y_metric(ax, metric)
            ax.tick_params(axis="x", rotation=45)
            fig.tight_layout()

            fname = (
                f"serie_{slugify(indicador)}_{slugify(metric)}_{slugify(estado)}.png"
            )
            fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
            plt.close(fig)


# ---------------------------------------------------------------------------
# 3. Rankings estatales (niveles y participación en el total)
# ---------------------------------------------------------------------------

def generar_rankings_estatales(df: pd.DataFrame, output_dir: Path):
    asegurar_directorio(output_dir)

    indicador = "vab_comercio_informal"
    metric = "Millones de pesos a precios de 2018"
    sub = filtrar_subset(df, indicador, metric)
    if sub.empty:
        return

    periodo_ref = elegir_periodo_referencia(sub)
    sub = sub[sub["periodo"] == periodo_ref]

    ultimo_anio = sub["anio"].max()

    sub_ultimo = sub[sub["anio"] == ultimo_anio]

    sub_estados = sub_ultimo[~sub_ultimo["es_nacional"]]
    sub_nacional = sub_ultimo[sub_ultimo["es_nacional"]]

    if sub_estados.empty or sub_nacional.empty:
        print("Advertencia: no hay datos suficientes para rankings en", ultimo_anio)
        return

    total_nacional = sub_nacional["valor"].sum()

    if total_nacional <= 0:
        print("Advertencia: total nacional <= 0 en", ultimo_anio, "- se omite ranking.")
        return

    resumen = (
        sub_estados.groupby("estado", as_index=False)["valor"]
        .sum()
        .sort_values("valor", ascending=False)
    )
    resumen["participacion_pct"] = 100 * resumen["valor"] / total_nacional

    # Top 10 por nivel
    top10 = resumen.head(10)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top10["estado"], top10["valor"])
    ax.invert_yaxis()

    ax.set_title(
        f"Top 10 estados por VAB del comercio informal\n"
        f"{ultimo_anio} – {metric} – periodo {periodo_ref}"
    )
    ax.set_xlabel("VAB (millones de pesos de 2018)")
    ax.xaxis.set_major_formatter(FuncFormatter(fmt_monetario_millones))
    for i, (v, pct) in enumerate(zip(top10["valor"], top10["participacion_pct"])):
        ax.text(v, i, f"{pct:.1f} %", va="center", ha="left", fontsize=9)

    fig.tight_layout()
    fname = f"ranking_vab_estados_top10_{ultimo_anio}.png"
    fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
    plt.close(fig)

    # Participación completa
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(resumen["estado"], resumen["participacion_pct"])
    ax.invert_yaxis()
    ax.set_title(
        f"Participación de cada estado en el VAB nacional del comercio informal\n"
        f"{ultimo_anio} – {metric} – periodo {periodo_ref}"
    )
    ax.set_xlabel("Participación (%)")
    ax.xaxis.set_major_formatter(FuncFormatter(fmt_porcentaje))
    fig.tight_layout()
    fname = f"participacion_vab_estados_{ultimo_anio}.png"
    fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# 4. Comparación dinámica nacional vs principales estados (trimestral)
# ---------------------------------------------------------------------------

def generar_comparaciones_dinamicas(df: pd.DataFrame, output_dir: Path):
    """
    Compara la trayectoria del VAB informal nacional con los principales estados
    usando la frecuencia original (trimestral). El eje x se construye con 'fecha'.
    """
    asegurar_directorio(output_dir)

    indicador = "vab_comercio_informal"
    metric = "Millones de pesos a precios de 2018"
    sub = filtrar_subset(df, indicador, metric)
    if sub.empty:
        return

    periodo_ref = elegir_periodo_referencia(sub)
    sub = sub[sub["periodo"] == periodo_ref].copy()

    # Orden por fecha
    sub = sub.sort_values(["estado", "fecha"])

    # Nacional y estatales
    serie_nacional = sub[sub["es_nacional"]].copy()
    sub_estados = sub[~sub["es_nacional"]].copy()

    if serie_nacional.empty or sub_estados.empty:
        return

    # Aseguramos unicidad por fecha
    serie_nacional = (
        serie_nacional.groupby("fecha", as_index=False)["valor"]
        .sum()
        .sort_values("fecha")
    )
    sub_estados = (
        sub_estados.groupby(["estado", "fecha"], as_index=False)["valor"]
        .sum()
        .sort_values(["estado", "fecha"])
    )
    sub_estados["anio"] = sub_estados["fecha"].dt.year

    # Último año disponible para ranking de top estados
    ultimo_anio = sub_estados["anio"].max()
    sub_ultimo = sub_estados[sub_estados["anio"] == ultimo_anio]

    if sub_ultimo.empty:
        return

    top5_estados = (
        sub_ultimo.groupby("estado", as_index=False)["valor"]
        .sum()
        .sort_values("valor", ascending=False)["estado"]
        .head(5)
        .tolist()
    )

    # Plot 1: nacional vs top 5 en niveles trimestrales
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        serie_nacional["fecha"],
        serie_nacional["valor"],
        marker="o",
        linewidth=2.0,
        label="Nacional",
    )

    for estado in top5_estados:
        sub_e = sub_estados[sub_estados["estado"] == estado]
        ax.plot(
            sub_e["fecha"],
            sub_e["valor"],
            marker="o",
            linewidth=1.2,
            label=estado,
        )

    ax.set_title(
        "VAB del comercio informal – Nacional vs principales estados\n"
        f"{metric} – periodo {periodo_ref}"
    )
    ax.set_xlabel("Fecha (trimestres)")
    ax.set_ylabel("VAB (millones de pesos de 2018)")
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_monetario_millones))
    ax.legend()
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()

    fname = "serie_nacional_vs_top5_estados_vab_trimestral.png"
    fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
    plt.close(fig)

    # Plot 2: participación trimestral de los top 5 en el total nacional
    total_nac_fecha = (
        serie_nacional[["fecha", "valor"]]
        .rename(columns={"valor": "valor_nac"})
        .copy()
    )

    participaciones = (
        sub_estados.merge(total_nac_fecha, on="fecha", how="left")
        .assign(participacion_pct=lambda x: 100 * x["valor"] / x["valor_nac"])
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    for estado in top5_estados:
        sub_e = participaciones[participaciones["estado"] == estado]
        ax.plot(
            sub_e["fecha"],
            sub_e["participacion_pct"],
            marker="o",
            linewidth=1.5,
            label=estado,
        )

    ax.set_title(
        "Participación de los principales estados en el VAB informal nacional\n"
        f"{metric} – periodo {periodo_ref}"
    )
    ax.set_xlabel("Fecha (trimestres)")
    ax.set_ylabel("Participación en el total nacional (%)")
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_porcentaje))
    ax.legend()
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()

    fname = "participacion_top5_estados_vab_trimestral.png"
    fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# 5. Heatmap estado–año (análisis espacial aproximado)
# ---------------------------------------------------------------------------

def generar_heatmap_estados(df: pd.DataFrame, output_dir: Path):
    asegurar_directorio(output_dir)

    indicador = "vab_comercio_informal"
    metric = "Millones de pesos a precios de 2018"
    sub = filtrar_subset(df, indicador, metric)
    sub = sub[~sub["es_nacional"]]
    if sub.empty:
        return

    periodo_ref = elegir_periodo_referencia(sub)
    sub = sub[sub["periodo"] == periodo_ref]

    sub_agg = (
        sub.groupby(["estado", "anio"], as_index=False)["valor"]
        .sum()
        .sort_values(["estado", "anio"])
    )

    tabla = sub_agg.pivot(index="estado", columns="anio", values="valor")

    tabla_norm = tabla.div(tabla.max(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(
        tabla_norm,
        cmap="viridis",
        ax=ax,
        cbar_kws={"label": "VAB relativo al máximo histórico del estado (máximo = 100)"},
    )
    ax.set_title(
        "Mapa de calor estado–año del VAB del comercio informal\n"
        f"Millones de pesos de 2018 – periodo {periodo_ref}"
    )
    ax.set_xlabel("Año")
    ax.set_ylabel("Estado")
    fig.tight_layout()

    fname = "heatmap_vab_estados_anio_normalizado.png"
    fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# 6. Series nacionales de variación porcentual anual
# ---------------------------------------------------------------------------

def generar_series_variacion_nacional(df: pd.DataFrame, output_dir: Path):
    asegurar_directorio(output_dir)

    combos_var = [
        ("vab_comercio_informal", "VariaciÃ³n porcentual anual"),
        ("remuneraciones_comercio_informal", "VariaciÃ³n porcentual anual"),
        ("puestos_trabajo_comercio_informal", "VariaciÃ³n porcentual anual"),
    ]

    for indicador, metric in combos_var:
        sub = filtrar_subset(df, indicador, metric)
        sub = sub[sub["es_nacional"]]
        if sub.empty:
            continue

        sub = sub.sort_values("fecha")

        fig, ax = plt.subplots()
        ax.plot(sub["fecha"], sub["valor"], marker="o", linewidth=1.8)

        ax.axhline(0, color="grey", linestyle="--", linewidth=1)
        ax.set_title(
            f"Variación porcentual anual – Serie nacional – {indicador.replace('_', ' ')}"
        )
        ax.set_xlabel("Fecha (trimestres)")
        ax.set_ylabel("Tasa de crecimiento anual (%)")
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_porcentaje))
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()

        fname = f"variacion_anual_nacional_{slugify(indicador)}.png"
        fig.savefig(output_dir / fname, dpi=300, bbox_inches="tight")
        plt.close(fig)


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------

def main():
    configurar_estilo()

    df = cargar_y_preparar_datos(DATA_PATH)

    # Subcarpetas de salida
    dir_series_nacionales = OUTPUT_PLOTS_DIR / "series_nacionales"
    dir_series_estatales = OUTPUT_PLOTS_DIR / "series_estatales"
    dir_rankings = OUTPUT_PLOTS_DIR / "rankings_estatales"
    dir_comparaciones = OUTPUT_PLOTS_DIR / "comparaciones"
    dir_heatmaps = OUTPUT_PLOTS_DIR / "heatmaps"
    dir_variacion = OUTPUT_PLOTS_DIR / "series_variacion_anual"

    print("\nGenerando series de tiempo nacionales...")
    generar_series_nacionales(df, dir_series_nacionales)

    print("Generando series de tiempo por estado...")
    generar_series_estatales(df, dir_series_estatales)

    print("Generando rankings y participaciones estatales...")
    generar_rankings_estatales(df, dir_rankings)

    print("Generando comparaciones dinámicas (nacional vs top estados, trimestral)...")
    generar_comparaciones_dinamicas(df, dir_comparaciones)

    print("Generando heatmap estado–año...")
    generar_heatmap_estados(df, dir_heatmaps)

    print("Generando series de variación porcentual anual (nacional)...")
    generar_series_variacion_nacional(df, dir_variacion)

    print("\nListo. Todas las gráficas se guardaron en:")
    print(OUTPUT_PLOTS_DIR)


if __name__ == "__main__":
    main()
