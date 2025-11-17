# parquet.py
# -*- coding: utf-8 -*-
"""
ENOE/ENOEN → 5 Parquets maestro por módulo (SIN renombrar columnas)
- Unión de columnas por módulo (VIVT, HOGT, SDEMT, COE1T, COE2T)
- Metadatos: anio (Int64), trimestre (Int64), anio_trimestre (string)
- Detección robusta de encoding (UTF-16/LE/BE, UTF-8, CP1252, Latin-1) y delimitador
- Carga en paralelo con Dask (delayed) y escritura en UN SOLO archivo Parquet por módulo
"""

import os, re, io, csv, glob, logging
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute

import pyarrow as pa
import pyarrow.parquet as pq

# ========= RUTAS =========
BASE_DIR  = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe"
FILES_DIR = os.path.join(BASE_DIR, "files")
OUT_DIR   = os.path.join(BASE_DIR, "parquet_master")
REPORTS_DIR = os.path.join(OUT_DIR, "reports")
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

MODULES = ["VIVT", "HOGT", "SDEMT", "COE1T", "COE2T"]

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ========= UTILIDADES =========
re_name = re.compile(r"_(20\d{2})_trim(\d)\b", flags=re.IGNORECASE)

def parse_year_trim_from_name(fname: str):
    m = re_name.search(fname)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

def find_col_ci(df: pd.DataFrame, target: str):
    low = target.lower()
    for c in df.columns:
        if c.lower() == low:
            return c
    return None

def derive_year_trim_from_per(df: pd.DataFrame):
    """Si no vienen en el nombre, intentar PER (dígito1=trimestre, dígitos2-3=año-2000)."""
    per_col = find_col_ci(df, "per")
    if per_col is None:
        n = len(df)
        return pd.Series([pd.NA]*n), pd.Series([pd.NA]*n)
    per = df[per_col].astype(str).str.replace(r"\D", "", regex=True).str.zfill(3)
    tri = pd.to_numeric(per.str[0], errors="coerce")
    yy  = pd.to_numeric(per.str[1:3], errors="coerce")
    anio = 2000 + yy
    return anio, tri

def add_meta_cols(df: pd.DataFrame, anio_from_name, tri_from_name):
    """Inserta meta y devuelve con dtypes CANÓNICOS: anio/trimestre=Int64, anio_trimestre=string."""
    if anio_from_name is None or tri_from_name is None:
        anio_ser, tri_ser = derive_year_trim_from_per(df)
    else:
        anio_ser = pd.Series([anio_from_name]*len(df))
        tri_ser  = pd.Series([tri_from_name]*len(df))

    df.insert(0, "anio", pd.to_numeric(anio_ser, errors="coerce").astype("Int64"))
    df.insert(1, "trimestre", pd.to_numeric(tri_ser, errors="coerce").astype("Int64"))

    try:
        str_dtype = pd.StringDtype()
    except Exception:
        str_dtype = "object"
    df.insert(
        2,
        "anio_trimestre",
        (df["anio"].astype("string") + "T" + df["trimestre"].astype("string")).astype(str_dtype)
    )
    return df

def detect_module_from_filename(fname: str):
    up = fname.upper()
    for tag in MODULES:
        if tag in up:
            return tag
    if "COE1" in up: return "COE1T"
    if "COE2" in up: return "COE2T"
    return None

# --- Detección de encoding por BOM + heurística ---
def sniff_encoding(path: str):
    with open(path, "rb") as f:
        head = f.read(4096)
    if head.startswith(b"\xEF\xBB\xBF"): return "utf-8-sig"
    if head.startswith(b"\xFF\xFE\x00\x00"): return "utf-32le"
    if head.startswith(b"\x00\x00\xFE\xFF"): return "utf-32be"
    if head.startswith(b"\xFF\xFE"): return "utf-16le"
    if head.startswith(b"\xFE\xFF"): return "utf-16be"
    if head[:2000].count(b"\x00") > 50: return "utf-16"
    return None

# --- Detección de delimitador sobre texto decodificado ---
def sniff_delimiter(text_sample: str):
    try:
        dialect = csv.Sniffer().sniff(text_sample, delimiters=[",",";","|","\t"])
        return dialect.delimiter
    except Exception:
        first_line = text_sample.splitlines()[0] if text_sample else ""
        candidates = [",",";","|","\t"]
        counts = {d: first_line.count(d) for d in candidates}
        return max(counts, key=counts.get) if counts else ","

def headers_only(path: str):
    """
    Devuelve: (cols:list[str], encoding:str, delimiter:str)
    Lee un bloque pequeño y prueba varios encodings hasta conseguir headers válidos.
    """
    enc0 = sniff_encoding(path)
    encodings_try = [e for e in [enc0, "utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16le", "utf-16be"] if e]
    with open(path, "rb") as f:
        sample = f.read(128 * 1024)

    for enc in encodings_try:
        try:
            text = sample.decode(enc, errors="replace")
        except Exception:
            continue
        delim = sniff_delimiter(text)
        # intentar nrows=0 sobre StringIO
        try:
            df0 = pd.read_csv(io.StringIO(text), sep=delim, nrows=0, dtype=str, engine="python")
            if len(df0.columns) > 0:
                return list(df0.columns), enc, delim
        except Exception:
            pass
        # intentar nrows=0 sobre archivo real
        try:
            df0 = pd.read_csv(path, sep=delim, nrows=0, dtype=str, engine="python", encoding=enc)
            if len(df0.columns) > 0:
                return list(df0.columns), enc, delim
        except Exception:
            continue

    # último intento: combos rápidos con archivo real
    for enc in encodings_try:
        for delim in [",",";","|","\t"]:
            try:
                df0 = pd.read_csv(path, sep=delim, nrows=0, dtype=str, engine="python", encoding=enc)
                if len(df0.columns) > 0:
                    return list(df0.columns), enc, delim
            except Exception:
                continue

    raise UnicodeDecodeError("failed", b"", 0, 1, "No se pudo decodificar con encodings probados")

# --- Lector completo robusto (por archivo) para usar con delayed ---
def read_full_csv_robust(path: str, enc: str, delim: str) -> pd.DataFrame:
    # 1) engine='c' rápido
    try:
        return pd.read_csv(path, sep=delim, dtype=str, engine="c", encoding=enc, low_memory=False)
    except Exception:
        pass
    # 2) engine='python' (sin low_memory)
    try:
        return pd.read_csv(path, sep=delim, dtype=str, engine="python", encoding=enc)
    except UnicodeDecodeError:
        # 3) re-try con encodings alternos
        for enc2 in ["utf-8-sig","utf-16","utf-16le","utf-16be","cp1252","latin-1"]:
            try:
                return pd.read_csv(path, sep=delim, dtype=str, engine="python", encoding=enc2)
            except Exception:
                continue
        # 4) lectura manual → reemplaza caracteres ilegales
        with open(path, "rb") as f:
            data = f.read()
        text = data.decode("latin-1", errors="replace")
        return pd.read_csv(io.StringIO(text), sep=delim, dtype=str, engine="python")
    except Exception:
        # fallback general
        with open(path, "rb") as f:
            data = f.read()
        text = data.decode("latin-1", errors="replace")
        return pd.read_csv(io.StringIO(text), sep=delim, dtype=str, engine="python")

# ========= PASADA 1: headers + meta por archivo =========
def pass1(csv_paths):
    info = []  # lista de dicts por archivo
    module_to_union = {m: set() for m in MODULES}
    module_to_files = {m: [] for m in MODULES}

    logging.info("Escaneando headers (pasada 1)")
    for path in csv_paths:
        fname = os.path.basename(path)
        mod = detect_module_from_filename(fname)
        if mod is None:
            logging.warning("No se infiere módulo para: %s (omitido)", fname)
            continue
        try:
            cols, enc, delim = headers_only(path)
            anio, tri = parse_year_trim_from_name(fname)
            info.append({"path": path, "mod": mod, "anio": anio, "tri": tri,
                         "encoding": enc, "delimiter": delim, "cols": cols})
            module_to_union[mod] |= set(cols)
            module_to_files[mod].append(path)
        except Exception as e:
            logging.error("Headers fallaron para %s: %s", fname, e)
    return info, module_to_union, module_to_files

# ========= Escritura en UN solo archivo Parquet =========
def write_single_parquet_from_ddf(ddf: dd.DataFrame, out_path: str):
    """
    Escribe un único archivo Parquet usando ParquetWriter, apilando particiones de Dask
    sin reventar memoria.
    """
    parts = ddf.to_delayed()
    if not parts:
        raise RuntimeError("No hay particiones que escribir.")

    # 1) Obtener primera partición para definir el esquema Arrow
    first_df = compute(parts[0])[0]
    table = pa.Table.from_pandas(first_df, preserve_index=False)
    writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")

    # 2) Escribir la primera
    writer.write_table(table)

    # 3) Escribir el resto secuencialmente
    for p in parts[1:]:
        pdf = compute(p)[0]
        tbl = pa.Table.from_pandas(pdf, preserve_index=False)
        # Validar columnas iguales
        if tbl.schema != writer.schema:
            # Alinear columnas que falten/sobren (por seguridad extra)
            cols_writer = [f.name for f in writer.schema]
            for c in cols_writer:
                if c not in pdf.columns:
                    pdf[c] = pd.NA
            pdf = pdf[cols_writer]
            tbl = pa.Table.from_pandas(pdf, preserve_index=False)
        writer.write_table(tbl)

    writer.close()

# ========= PASADA 2: construcción con Dask =========
def build_with_dask(file_info, module_to_union):
    # dtypes canónicos para meta:
    try:
        string_dtype = pd.StringDtype()
    except Exception:
        string_dtype = "object"

    for mod in MODULES:
        files_mod = [fi for fi in file_info if fi["mod"] == mod]
        if not files_mod:
            continue

        # Esquema final: meta + unión alfabética (sin renombrar)
        union_cols = sorted(module_to_union[mod], key=str.lower)
        final_cols = ["anio", "trimestre", "anio_trimestre"] + [c for c in union_cols if c not in {"anio","trimestre","anio_trimestre"}]

        # META con dtypes explícitos
        meta_dict = {}
        for c in final_cols:
            if c in ("anio", "trimestre"):
                meta_dict[c] = pd.Series(dtype="Int64")
            elif c == "anio_trimestre":
                meta_dict[c] = pd.Series(dtype=string_dtype)
            else:
                meta_dict[c] = pd.Series(dtype=string_dtype)   # fuerza string en no-meta
        meta = pd.DataFrame(meta_dict)[final_cols]

        delayed_frames = []
        for fi in files_mod:
            path, enc, delim, anio, tri = fi["path"], fi["encoding"], fi["delimiter"], fi["anio"], fi["tri"]

            @delayed
            def load_one(path=path, enc=enc, delim=delim, anio=anio, tri=tri, final_cols=final_cols, string_dtype=string_dtype):
                df = read_full_csv_robust(path, enc, delim)

                # Metadatos
                df = add_meta_cols(df, anio, tri)

                # Añadir columnas faltantes y ordenar
                for c in final_cols:
                    if c not in df.columns:
                        df[c] = pd.NA
                df = df.reindex(columns=final_cols)

                # Casteos DEFINITIVOS para que todas las particiones coincidan:
                df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
                df["trimestre"] = pd.to_numeric(df["trimestre"], errors="coerce").astype("Int64")
                df["anio_trimestre"] = (df["anio"].astype("string") + "T" + df["trimestre"].astype("string")).astype(string_dtype)

                # Todo lo demás como string (evita conflictos de esquema entre particiones)
                non_meta = [c for c in df.columns if c not in ("anio","trimestre","anio_trimestre")]
                for c in non_meta:
                    df[c] = df[c].astype(string_dtype)

                return df

            delayed_frames.append(load_one())

        ddf = dd.from_delayed(delayed_frames, meta=meta)

        # === Escribir UN solo archivo Parquet ===
        out_path = os.path.join(OUT_DIR, f"enoe_master_{mod.lower()}.parquet")
        logging.info("Escribiendo único Parquet de %s → %s", mod, out_path)
        write_single_parquet_from_ddf(ddf, out_path)

        # Resumen mínimo
        nrows = ddf.shape[0].compute()
        ncols = len(ddf.columns)
        logging.info("Parquet %s listo | Filas=%s, Cols=%s -> %s", mod, f"{nrows:,}", ncols, out_path)

        # Cobertura de columnas (cuántos archivos la traen)
        coverage = []
        for col in final_cols:
            cnt = sum(1 for fi in files_mod if col in set(fi["cols"]))
            coverage.append((col, cnt, len(files_mod)))
        cov_df = pd.DataFrame(coverage, columns=["columna","archivos_con_col","total_archivos"])
        cov_csv = os.path.join(REPORTS_DIR, f"coverage_{mod.lower()}.csv")
        cov_df.to_csv(cov_csv, index=False, encoding="utf-8")

if __name__ == "__main__":
    csv_files = sorted(glob.glob(os.path.join(FILES_DIR, "*.csv")))
    if not csv_files:
        logging.error("NO se hallaron CSV en %s", FILES_DIR)
        raise SystemExit(1)

    logging.info("CSVs detectados: %d", len(csv_files))
    info, module_to_union, module_to_files = pass1(csv_files)
    build_with_dask(info, module_to_union)
    logging.info("Listo. Revisa: %s", OUT_DIR)
