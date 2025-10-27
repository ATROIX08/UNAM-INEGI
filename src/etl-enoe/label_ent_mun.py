# label_ent_mun.py
# -*- coding: utf-8 -*-
"""
Etiqueta ENT (Entidad) y MUN (Municipio) en Parquet ENOE/ENOEN usando catálogo AGEEML.

- VIVT / HOGT / SDEMT: agrega ent_nombre y mun_nombre
- COE1T / COE2T: agrega solo ent_nombre (no traen MUN)
- No borra columnas originales salvo que lo configures

Requisitos:
  conda install -c conda-forge dask pyarrow pandas
"""

import os, io, csv, logging
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute
import pyarrow as pa
import pyarrow.parquet as pq

# =================== CONFIG ===================
BASE_DIR = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe"

CATALOGO_PATH = os.path.join(
    BASE_DIR,
    r"descompressed\catun_municipio\AGEEML_20251021625902_UTF.csv"
)

PARQUETS_IN = [
    os.path.join(BASE_DIR, r"parquet_master\enoe_master_coe1t.parquet"),
    os.path.join(BASE_DIR, r"parquet_master\enoe_master_coe2t.parquet"),
    os.path.join(BASE_DIR, r"parquet_master\enoe_master_hogt.parquet"),
    os.path.join(BASE_DIR, r"parquet_master\enoe_master_sdemt.parquet"),
    os.path.join(BASE_DIR, r"parquet_master\enoe_master_vivt.parquet"),
]

OUT_DIR = os.path.join(BASE_DIR, r"parquet_master_labeled")
os.makedirs(OUT_DIR, exist_ok=True)

# Si quieres eliminar las columnas de código (ent, mun) tras etiquetar:
DROP_ORIGINAL_CODES = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


# =================== HELPERS ===================
def sniff_delimiter(sample_text: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",",";","|","\t"])
        return dialect.delimiter
    except Exception:
        first = sample_text.splitlines()[0] if sample_text else ""
        candidates = [",",";","|","\t"]
        counts = {d: first.count(d) for d in candidates}
        return max(counts, key=counts.get) if counts else ","


def read_catalog(catalog_path: str):
    """
    Lee el catálogo AGEEML (CSV) y devuelve:
      - ent_df:  columnas ['CVE_ENT','NOM_ENT'] únicas
      - muni_df: columnas ['CVE_ENT','CVE_MUN','NOM_MUN'] únicas
    Castea claves como strings de longitud fija: ENT=2, MUN=3
    """
    with open(catalog_path, "rb") as f:
        sample = f.read(256 * 1024)
    # decodificación “optimista”
    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1"]:
        try:
            text = sample.decode(enc, errors="replace")
            sep = sniff_delimiter(text)
            df = pd.read_csv(catalog_path, encoding=enc, sep=sep, dtype=str, engine="python")
            break
        except Exception:
            df = None
    if df is None:
        # Último intento: latin-1
        df = pd.read_csv(catalog_path, encoding="latin-1", sep=",", dtype=str, engine="python")

    # Normalizar nombres esperados
    expected = {"CVE_ENT", "NOM_ENT", "CVE_MUN", "NOM_MUN"}
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Catálogo no tiene columnas esperadas: faltan {missing}")

    # Normalizar claves
    df["CVE_ENT"] = df["CVE_ENT"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(2)
    df["CVE_MUN"] = df["CVE_MUN"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(3)

    ent_df = df[["CVE_ENT", "NOM_ENT"]].drop_duplicates(subset=["CVE_ENT"]).reset_index(drop=True)
    muni_df = df[["CVE_ENT", "CVE_MUN", "NOM_MUN"]].drop_duplicates(subset=["CVE_ENT","CVE_MUN"]).reset_index(drop=True)

    logging.info("Catálogo cargado: ENT=%d, MUN=%d", len(ent_df), len(muni_df))
    return ent_df, muni_df


def find_col_case_insensitive(cols, target):
    """Devuelve el nombre real de la columna si existe sin distinguir mayúsculas/minúsculas."""
    tl = target.lower()
    for c in cols:
        if c.lower() == tl:
            return c
    return None


def write_single_parquet_from_ddf(ddf: dd.DataFrame, out_path: str):
    """
    Escribe un único archivo Parquet agregando particiones de Dask.
    Evita consumir mucha RAM.
    """
    parts = ddf.to_delayed()
    if not parts:
        raise RuntimeError("No hay particiones que escribir.")

    # primera partición para fijar esquema
    first_df = compute(parts[0])[0]
    table = pa.Table.from_pandas(first_df, preserve_index=False)
    writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")

    writer.write_table(table)
    for p in parts[1:]:
        pdf = compute(p)[0]
        tbl = pa.Table.from_pandas(pdf, preserve_index=False)
        if tbl.schema != writer.schema:
            # Alinear por seguridad (no debería ocurrir)
            cols_writer = [f.name for f in writer.schema]
            for c in cols_writer:
                if c not in pdf.columns:
                    pdf[c] = pd.NA
            pdf = pdf[cols_writer]
            tbl = pa.Table.from_pandas(pdf, preserve_index=False)
        writer.write_table(tbl)

    writer.close()


def label_one_parquet(parquet_path: str, ent_df: pd.DataFrame, muni_df: pd.DataFrame):
    """
    Etiqueta ENT (siempre) y MUN (si existe) en un Parquet.
    Crea nuevas columnas: ent_nombre, mun_nombre.
    """
    fname = os.path.basename(parquet_path)
    out_path = os.path.join(OUT_DIR, fname.replace(".parquet", "_labeled.parquet"))

    logging.info("Leyendo: %s", parquet_path)
    ddf = dd.read_parquet(parquet_path, engine="pyarrow")

    # ----- ENT -----
    ent_col = find_col_case_insensitive(ddf.columns, "ent")
    if ent_col:
        ddf = ddf.assign(
            _ent_code=ddf[ent_col].astype("string").str.replace(r"\D", "", regex=True).str.zfill(2)
        )
        # merge con catálogo de ENT (pandas DF se broadcast a Dask)
        ddf = ddf.merge(ent_df, how="left", left_on="_ent_code", right_on="CVE_ENT")
        # renombrar
        ddf = ddf.rename(columns={"NOM_ENT": "ent_nombre"})
        # limpiar columnas auxiliares
        ddf = ddf.drop(columns=["CVE_ENT"])
    else:
        logging.warning("No se encontró columna ENT en %s (saltando ENT)", fname)

    # ----- MUN (solo VIVT/HOGT/SDEMT) -----
    mun_col = find_col_case_insensitive(ddf.columns, "mun")
    if mun_col:
        # necesitamos _ent_code para join (asegúrate que existe, si no, créalo vacío)
        if "_ent_code" not in ddf.columns:
            ddf = ddf.assign(
                _ent_code=ddf[ent_col].astype("string").str.replace(r"\D", "", regex=True).str.zfill(2)
                if ent_col else dd.from_pandas(pd.Series([], dtype="string"), npartitions=1)
            )
        ddf = ddf.assign(
            _mun_code=ddf[mun_col].astype("string").str.replace(r"\D", "", regex=True).str.zfill(3)
        )
        # merge con catálogo de municipios
        ddf = ddf.merge(muni_df, how="left",
                        left_on=["_ent_code", "_mun_code"],
                        right_on=["CVE_ENT", "CVE_MUN"])
        ddf = ddf.rename(columns={"NOM_MUN": "mun_nombre"}).drop(columns=["CVE_ENT", "CVE_MUN"])
    else:
        logging.info("La tabla %s no contiene MUN; se omite mun_nombre.", fname)

    # ----- limpiar helpers -----
    drop_cols = []
    if "_ent_code" in ddf.columns: drop_cols.append("_ent_code")
    if "_mun_code" in ddf.columns: drop_cols.append("_mun_code")
    if drop_cols:
        ddf = ddf.drop(columns=drop_cols)

    # ----- (opcional) eliminar códigos originales -----
    if DROP_ORIGINAL_CODES:
        todrop = []
        if ent_col and ent_col in ddf.columns: todrop.append(ent_col)
        if mun_col and mun_col in ddf.columns: todrop.append(mun_col)
        if todrop:
            ddf = ddf.drop(columns=todrop)

    # ----- escribir un único parquet -----
    logging.info("Escribiendo: %s", out_path)
    write_single_parquet_from_ddf(ddf, out_path)
    # pequeño resumen
    nrows = ddf.shape[0].compute()
    logging.info("Listo %s | Filas=%s", out_path, f"{nrows:,}")


# =================== MAIN ===================
if __name__ == "__main__":
    # 1) catálogo de ENT/MUN
    ent_df, muni_df = read_catalog(CATALOGO_PATH)

    # 2) aplicar a cada parquet
    for p in PARQUETS_IN:
        if os.path.exists(p):
            label_one_parquet(p, ent_df, muni_df)
        else:
            logging.warning("No existe el Parquet: %s (saltando)", p)

    logging.info("¡Terminado! Revisa: %s", OUT_DIR)

import dask.dataframe as dd


p = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\parquet_master_labeled\enoe_master_sdemt_labeled.parquet"
ddf = dd.read_parquet(p)

# ¿Cuántas filas quedaron sin nombre de entidad?
print(ddf["ent_nombre"].isna().sum().compute())

# Top 10 entidades por conteo
print(ddf["ent_nombre"].value_counts().head(10))

# Para municipios (en VIVT/HOGT/SDEMT)
print(ddf["mun_nombre"].isna().sum().compute())
print(ddf["mun_nombre"].value_counts().head(10))
