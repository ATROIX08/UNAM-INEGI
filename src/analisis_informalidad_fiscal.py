import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib
# Forzar uso de backend sin interfaz gráfica para evitar errores al guardar
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ---------------------------------------------------------------------------
# 1. CONFIGURACIÓN Y RUTAS
# ---------------------------------------------------------------------------
BASE_DIR = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI"

# Inputs
PATH_MEITEF = os.path.join(BASE_DIR, "data", "meitef_tidy", "meitef_comercio_informal_tidy_ALL.csv")
PATH_IMPUESTOS = os.path.join(BASE_DIR, "output", "impuestos_data_clean_trimestral.csv")
PATH_IMSS = os.path.join(BASE_DIR, "output", "ingreso_obrero_patronal_trimestral.csv")

# Output
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "analisis_impacto_fiscal_completo")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Estilo
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["figure.dpi"] = 150


# ---------------------------------------------------------------------------
# FUNCIÓN AUXILIAR: IMPRIMIR TEMPORALIDAD
# ---------------------------------------------------------------------------
def imprimir_temporalidad(df, nombre="Serie"):
    if df is None or df.empty:
        print(f"    {nombre}: dataframe vacío, no hay temporalidad.")
        return

    inicio = df.index.min()
    fin = df.index.max()
    n = len(df)

    # Frecuencia aproximada (solo para contexto)
    # Si son trimestrales, el salto típico será ~90 días
    difs = df.index.to_series().diff().dropna()
    freq_dias = difs.median().days if not difs.empty else None

    print(f"    Temporalidad {nombre}:")
    print(f"      Inicio: {inicio.date()}")
    print(f"      Fin:    {fin.date()}")
    print(f"      Obs.:   {n}")
    if freq_dias is not None:
        print(f"      Salto mediano entre obs.: {freq_dias} días (aprox. trimestral si ~90)")
    print("")


# ---------------------------------------------------------------------------
# 2. CARGA Y UNIFICACIÓN DE DATOS
# ---------------------------------------------------------------------------
def cargar_datos():
    print("--> [1/6] Cargando y procesando datos...")

    # A) MEITEF (Comercio Informal)
    try:
        df_vab = pd.read_csv(PATH_MEITEF)
    except FileNotFoundError:
        print(f"ERROR: No se encontró {PATH_MEITEF}")
        exit()

    if "ï»¿estado" in df_vab.columns:
        df_vab.rename(columns={"ï»¿estado": "estado"}, inplace=True)

    # Filtros: Nacional + VAB + Precios Corrientes
    df_vab = df_vab[
        (df_vab["estado"] == "Estados Unidos Mexicanos") &
        (df_vab["indicador"] == "vab_comercio_informal") &
        (df_vab["metric"] == "Millones de pesos a precios corrientes")
    ].copy()

    # Lógica de Fechas Trimestrales
    def periodo_a_mes(p):
        p = str(p)
        if "T1" in p: return 3
        if "T2" in p: return 6
        if "T3" in p: return 9
        return 12  # T4

    df_vab["mes"] = df_vab["periodo"].apply(periodo_a_mes)
    df_vab["Fecha"] = (
        pd.to_datetime(df_vab["anio"].astype(str) + "-" + df_vab["mes"].astype(str) + "-01")
        + pd.offsets.MonthEnd(0)
    )

    df_meitef = (
        df_vab[["Fecha", "valor"]]
        .rename(columns={"valor": "VAB_Informal"})
        .set_index("Fecha")
        .sort_index()
    )

    imprimir_temporalidad(df_meitef, "MEITEF (VAB Informal)")

    # B) Datos Fiscales (SAT e IMSS)
    try:
        df_sat = pd.read_csv(PATH_IMPUESTOS, parse_dates=["Fecha"], index_col="Fecha").sort_index()
        df_imss = pd.read_csv(PATH_IMSS, parse_dates=["Fecha"], index_col="Fecha").sort_index()
    except FileNotFoundError:
        print("ERROR: Faltan los archivos de impuestos o IMSS en la carpeta output.")
        exit()

    imprimir_temporalidad(df_sat, "SAT (ISR/IVA)")
    imprimir_temporalidad(df_imss, "IMSS (Ingreso obrero-patronal)")

    # C) Merge (Inner Join)
    df_master = df_meitef.join([df_sat, df_imss], how="inner")
    df_master.rename(columns={
        "Ingreso obrero - patronal nacional": "IMSS",
        "ISR": "ISR",
        "IVA": "IVA"
    }, inplace=True)

    imprimir_temporalidad(df_master, "Panel final (inner join)")

    print(f"    Datos consolidados correctamente: {len(df_master)} observaciones trimestrales.")
    return df_master


# ---------------------------------------------------------------------------
# 3. FUNCIONES DE VISUALIZACIÓN EXPLORATORIA
# ---------------------------------------------------------------------------
def graficar_correlacion(df):
    path = os.path.join(OUTPUT_DIR, "1_Matriz_Correlacion.png")
    print(f"--> [2/6] Generando Matriz de Correlación: {path}")

    plt.figure(figsize=(8, 6))
    corr = df.corr()
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
    plt.title("1. Matriz de Correlación: Informalidad vs Recaudación (Valores Nominales)", fontsize=12)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def graficar_series_tiempo(df):
    path = os.path.join(OUTPUT_DIR, "2_Series_Tiempo_Base100.png")
    print(f"--> [3/6] Generando Series de Tiempo: {path}")

    df_base100 = df.apply(lambda x: (x / x.iloc[0]) * 100)
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_base100, linewidth=2.5)
    plt.title("2. Evolución Comparativa (Índice Base 100 = Inicio del Periodo)", fontsize=14)
    plt.ylabel("Índice de Crecimiento")
    plt.xlabel("Año")
    plt.legend(title="Variable")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def graficar_dispersiones_simples(df):
    path = os.path.join(OUTPUT_DIR, "3_Dispersiones_Simples.png")
    print(f"--> [4/6] Generando Dispersiones Simples: {path}")

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    vars_y = ["ISR", "IVA", "IMSS"]
    colors = ["green", "blue", "orange"]

    for i, var in enumerate(vars_y):
        sns.regplot(x="VAB_Informal", y=var, data=df, ax=axes[i],
                    color=colors[i], scatter_kws={"alpha": 0.5})
        axes[i].set_title(f"Relación VAB Informal vs {var}")
        axes[i].set_xlabel("VAB Informal ($)")
        axes[i].set_ylabel(f"{var} ($)")

    plt.suptitle("3. Dispersión Simple y Tendencia Lineal", fontsize=16)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def graficar_ciclos(df):
    path = os.path.join(OUTPUT_DIR, "4_Ciclos_Variacion_Anual.png")
    print(f"--> [5/6] Generando Ciclos Económicos: {path}")

    df_pct = df.pct_change(4).dropna() * 100  # Variación Anual
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_pct, linewidth=2)
    plt.axhline(0, color="black", linestyle="--", linewidth=1)
    plt.title("4. Ciclos Económicos: Variación Porcentual Anual (%)", fontsize=14)
    plt.ylabel("Crecimiento Anual (%)")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


# ---------------------------------------------------------------------------
# 4. ANÁLISIS ECONOMÉTRICO Y VISUALIZACIÓN DE MODELOS
# ---------------------------------------------------------------------------
def analisis_econometrico(df):
    print("--> [6/6] Ejecutando modelos econométricos y generando últimas gráficas...")

    df_log = np.log(df[["VAB_Informal", "ISR", "IVA", "IMSS"]])

    vars_dep = ["ISR", "IVA", "IMSS"]
    modelos_resumen = {}
    full_summaries = ""

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    colors = {"ISR": "green", "IVA": "blue", "IMSS": "orange"}

    for i, var in enumerate(vars_dep):
        Y = df_log[var]
        X = df_log["VAB_Informal"]
        X_const = sm.add_constant(X)

        model = sm.OLS(Y, X_const).fit()

        beta = model.params["VAB_Informal"]
        alpha = model.params["const"]
        r2 = model.rsquared
        modelos_resumen[var] = beta

        full_summaries += f"\n{'='*60}\nMODELO: Log({var}) vs Log(Informalidad)\n{'='*60}\n"
        full_summaries += str(model.summary()) + "\n\n"

        ax = axes[i]
        ax.scatter(X, Y, color=colors[var], alpha=0.6, label="Datos Observados")

        x_pred = np.linspace(X.min(), X.max(), 100)
        y_pred = alpha + beta * x_pred
        ax.plot(x_pred, y_pred, color="red", linewidth=2, label="Ajuste OLS")

        ax.set_title(f"Modelo {var}\nElasticidad (β)={beta:.2f} | R²={r2:.2f}")
        ax.set_xlabel("Log(VAB Informal)")
        ax.set_ylabel(f"Log({var})")
        ax.legend()

        eq_text = f"$ln(y) = {alpha:.1f} + {beta:.2f} \\cdot ln(x)$"
        ax.text(0.05, 0.90, eq_text, transform=ax.transAxes, fontsize=10,
                bbox=dict(facecolor="white", alpha=0.8))

    plt.suptitle("5. Modelos Econométricos: Elasticidad Fiscal ante la Informalidad", fontsize=16)
    plt.tight_layout()
    path_5 = os.path.join(OUTPUT_DIR, "5_Modelos_Regresion_Ajuste.png")
    plt.savefig(path_5)
    plt.close()
    print(f"    Gráfica guardada: {path_5}")

    plt.figure(figsize=(10, 6))
    betas = pd.Series(modelos_resumen)
    cols = [colors[k] for k in betas.index]
    bars = plt.bar(betas.index, betas.values, color=cols, alpha=0.8)
    plt.axhline(1, color="gray", linestyle="--", label="Elasticidad Unitaria (1.0)")
    plt.title("6. Resumen de Sensibilidad (Elasticidades)", fontsize=14)
    plt.ylabel("Coeficiente Beta (% cambio Y / 1% cambio X)")

    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, h + 0.02, f"{h:.2f}",
                 ha="center", fontweight="bold")

    plt.legend()
    plt.tight_layout()
    path_6 = os.path.join(OUTPUT_DIR, "6_Resumen_Elasticidades.png")
    plt.savefig(path_6)
    plt.close()
    print(f"    Gráfica guardada: {path_6}")

    return full_summaries, betas


# ---------------------------------------------------------------------------
# 5. GENERACIÓN DE REPORTE DE TEXTO Y POLÍTICA PÚBLICA
# ---------------------------------------------------------------------------
def generar_reporte_texto(df, summaries, betas):
    print("--> Generando reporte técnico final...")

    filepath = os.path.join(OUTPUT_DIR, "REPORTE_TECNICO_Y_POLITICA.txt")

    vab_prom = df["VAB_Informal"].mean()
    imss_prom = df["IMSS"].mean()
    ratio = vab_prom / imss_prom

    df_pct = df.pct_change(4).dropna()
    corr_ciclos = df_pct.corr()["VAB_Informal"]

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("================================================================================\n")
        f.write("   ANÁLISIS ECONÓMICO: INFORMALIDAD Y RECAUDACIÓN EN MÉXICO\n")
        f.write("================================================================================\n\n")

        f.write("1. DIAGNÓSTICO DE MAGNITUD (Promedios Trimestrales)\n")
        f.write("-" * 60 + "\n")
        f.write(f"VAB Comercio Informal: ${vab_prom:,.0f} Millones MXN\n")
        f.write(f"Recaudación IMSS:      ${imss_prom:,.0f} Millones MXN\n")
        f.write(f"RATIO: El sector informal es {ratio:.2f} veces más grande que la recaudación del IMSS.\n\n")

        f.write("2. ANÁLISIS DE CICLOS ECONÓMICOS (Variación Anual)\n")
        f.write("-" * 60 + "\n")
        f.write("Correlación entre el crecimiento del VAB Informal y la Recaudación:\n")
        f.write(f" - vs ISR:  {corr_ciclos['ISR']:.4f}\n")
        f.write(f" - vs IVA:  {corr_ciclos['IVA']:.4f}\n")
        f.write(f" - vs IMSS: {corr_ciclos['IMSS']:.4f}\n")
        f.write("INTERPRETACIÓN: La correlación cercana a cero (o negativa) en impuestos como el ISR\n")
        f.write("indica una desconexión estructural. El fisco no ve los flujos del comercio informal.\n\n")

        f.write("3. RESULTADOS ECONOMÉTRICOS (Regresión OLS Log-Log)\n")
        f.write("-" * 60 + "\n")
        f.write(summaries)

        f.write("\n================================================================================\n")
        f.write("   PROPUESTA DE POLÍTICA PÚBLICA: 'TRAZABILIDAD FISCAL AGUAS ABAJO'\n")
        f.write("================================================================================\n\n")

        f.write("TÍTULO DE LA INICIATIVA:\n")
        f.write("Programa de Formalización Digital de la Cadena de Suministro (PFD-CS).\n\n")

        f.write("CONTEXTO Y JUSTIFICACIÓN:\n")
        f.write("El análisis cuantitativo demuestra que, aunque el sector de comercio informal genera un Valor Agregado\n")
        f.write("Bruto promedio de 1.7 billones de pesos trimestrales (21 veces la recaudación del IMSS), existe una\n")
        f.write("correlación nula (-0.05) entre sus ciclos económicos y la recaudación de ISR. Esto evidencia que los\n")
        f.write("mecanismos actuales de fiscalización, centrados en la emisión de facturas por parte del proveedor, pierden\n")
        f.write("la trazabilidad en el último eslabón de la cadena: la venta al 'Público en General'.\n\n")

        f.write("OBJETIVO:\n")
        f.write("Impulsar la transición a la formalidad mediante la 'Trazabilidad Forward-Link' (hacia adelante),\n")
        f.write("utilizando a los grandes distribuidores como catalizadores de la regularización de sus clientes.\n\n")

        f.write("MECANISMO DE IMPLEMENTACIÓN:\n\n")

        f.write("1. RESTRICCIÓN DE FACTURACIÓN GENÉRICA PARA GRANDES CONTRIBUYENTES:\n")
        f.write("   Se implementará una normativa progresiva que limite el porcentaje de ventas que los Grandes\n")
        f.write("   Contribuyentes (distribuidores mayoristas y empresas de consumo masivo) pueden facturar bajo el RFC\n")
        f.write("   genérico de 'Público en General'. Para poder venderle a sus clientes comerciales (tienditas, puestos),\n")
        f.write("   será requisito vincular la venta a un RFC válido.\n\n")

        f.write("2. TECNOLOGÍA DE FRICCIÓN MÍNIMA (QR - CIF):\n")
        f.write("   Para evitar que la burocracia paralice el comercio, la identificación se realizará mediante el escaneo\n")
        f.write("   del código QR de la Cédula de Identificación Fiscal (CIF). El pequeño comerciante podrá tener su CIF\n")
        f.write("   impresa o digital. El repartidor/vendedor del distribuidor escaneará el QR al momento de la entrega,\n")
        f.write("   vinculando automáticamente el CFDI de ingreso sin necesidad de intercambio manual de datos.\n\n")

        f.write("3. INCENTIVOS DE CUMPLIMIENTO (PUSH & PULL):\n")
        f.write("   A) Para el Distribuidor (Grandes Empresas): Se otorgarán beneficios fiscales, como depreciación acelerada\n")
        f.write("      de activos logísticos, a aquellas empresas que logren identificar con RFC al 90% de su cartera de clientes.\n")
        f.write("   B) Para el Pequeño Comerciante: Al realizar su primera compra identificada, el sistema lo pre-inscribirá\n")
        f.write("      automáticamente en el Régimen Simplificado de Confianza (RESICO), otorgándole un periodo de gracia\n")
        f.write("      de 6 meses sin obligaciones de pago, para facilitar su adaptación administrativa.\n\n")

        f.write("4. FISCALIZACIÓN INTELIGENTE (DETECCIÓN DE DISCREPANCIAS):\n")
        f.write("   Al digitalizar la venta del proveedor, el SAT obtendrá información precisa sobre el costo de ventas e\n")
        f.write("   inventarios del pequeño comercio. Si un RFC (Persona Física) registra compras de insumos por montos\n")
        f.write("   elevados pero declara ingresos de cero o inexistentes, se activarán alertas automáticas por Discrepancia\n")
        f.write("   Fiscal, permitiendo auditorías dirigidas y eficientes.\n\n")

        f.write("IMPACTO ESPERADO:\n")
        f.write("Aumentar la base de contribuyentes activos, reducir la evasión en el IVA trasladado y alinear el crecimiento\n")
        f.write("de la recaudación tributaria con el verdadero dinamismo del sector comercio.\n")

    print(f"--> Reporte guardado: {filepath}")


# ---------------------------------------------------------------------------
# 6. EJECUCIÓN PRINCIPAL
# ---------------------------------------------------------------------------
def main():
    df = cargar_datos()

    graficar_correlacion(df)
    graficar_series_tiempo(df)
    graficar_dispersiones_simples(df)
    graficar_ciclos(df)

    resumen_texto, betas = analisis_econometrico(df)

    generar_reporte_texto(df, resumen_texto, betas)

    print("\n========================================")
    print("      ¡ANÁLISIS COMPLETADO CON ÉXITO!      ")
    print("========================================")
    print(f"Verifica la carpeta de salida:\n{OUTPUT_DIR}")


if __name__ == "__main__":
    main()
