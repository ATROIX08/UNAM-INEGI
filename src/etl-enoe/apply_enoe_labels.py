# -*- coding: utf-8 -*-
r"""
Aplica etiquetas (categorías) de los diccionarios RNM (JSON) a parquets ENOE.

- Consolida múltiples JSON por tabla/trim para crear un mapeo unificado por base:
  COE1T, COE2T, SDEMT, HOGT, VIVT.
- Crea nuevas columnas <col>_label con las descripciones.
- Sin joins (evita ColumnNotFoundError): usa UDF con normalización robusta.
- Matching case-insensitive de nombres (JSON vs Parquet).
- Diagnóstico:
  * columnas del parquet -> __columns.txt
  * missing_in_parquet
  * label_all_null_detail con ejemplos de códigos no mapeados.

Ejecuta sin argumentos: usa las rutas embebidas abajo.
"""

import re
import os
import json
import glob
from collections import defaultdict
from typing import Dict, List, Tuple, Any

import polars as pl

# ============================================================
# 1) RUTAS POR DEFECTO (EDITA AQUÍ SI CAMBIAN)
# ============================================================
DEFAULT_JSON_ROOT = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\UNAM-INEGI"
DEFAULT_PARQUET_DIR = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\parquet_master_labeled"
DEFAULT_OUT_DIR = r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\parquet_master_labeled_labels"

PARQUET_PATTERNS = [
    "enoe_master_coe1t_labeled.parquet",
    "enoe_master_coe2t_labeled.parquet",
    "enoe_master_sdemt_labeled.parquet",
    "enoe_master_hogt_labeled.parquet",
    "enoe_master_vivt_labeled.parquet",
]

# ============================================================
# 2) UTILIDADES: parsing y consolidación de diccionarios
# ============================================================
# Ejemplos válidos: COE2T424, COE1T324, SDEMT324, HOGT424, VIVT224
TABLA_BASE_RE = re.compile(r"^([A-Z0-9]+?)(\d)(\d{2})$")

def parse_tabla_base(tabla: str) -> Tuple[str, int, int]:
    """
    Devuelve (base_canon, trimestre, año_2d).
    base_canon ∈ {COE1T, COE2T, SDEMT, HOGT, VIVT}
    """
    s = (tabla or "").strip().upper()
    m = TABLA_BASE_RE.match(s)
    if m:
        base_pref, tri, yy = m.group(1), int(m.group(2)), int(m.group(3))
        if base_pref in {"COE1", "COE1T"}: base_pref = "COE1T"
        elif base_pref in {"COE2", "COE2T"}: base_pref = "COE2T"
        elif base_pref in {"SDEM", "SDEMT"}: base_pref = "SDEMT"
        elif base_pref in {"HOG", "HOGT"}:   base_pref = "HOGT"
        elif base_pref in {"VIV", "VIVT"}:   base_pref = "VIVT"
        return base_pref, tri, yy
    base = re.sub(r"[^A-Z0-9]", "", s)
    return base, 0, 0

def year_quarter_rank(tri: int, yy: int) -> int:
    return (2000 + int(yy)) * 10 + int(tri)

def normalize_code_keys(code: str) -> List[str]:
    """
    Claves equivalentes robustas para el mapping: original, sin ceros a la izquierda y 2 dígitos.
    Para códigos no numéricos: agrega variante upper (p.ej. 'a' -> 'A').
    """
    keys = set()
    s = str(code).strip()
    if s == "":
        return []
    keys.add(s)
    # variante upper para strings no numéricos
    if not re.fullmatch(r"-?\d+", s):
        keys.add(s.upper())
    # variantes numéricas
    if re.fullmatch(r"-?\d+", s):
        try:
            n = int(s)
            keys.add(str(n))        # "01" -> "1"
            if 0 <= n < 100:
                keys.add(f"{n:02d}")  # "1" -> "01"
        except ValueError:
            pass
    return list(keys)

def find_all_json_files(root: str) -> List[str]:
    pattern = os.path.join(root, "**", "rnm_diccionario_*.json")
    files = glob.glob(pattern, recursive=True)
    files = [
        f for f in files
        if f.lower().endswith(".json")
        and os.path.basename(f).lower().startswith("rnm_diccionario_")
        and not os.path.basename(f).lower().startswith("rnm_diccionario_test")
    ]
    comp = os.path.join(root, "rnm_diccionario_completo_ALL.json")
    if os.path.exists(comp):
        files.append(comp)
    # Quitar duplicados preservando orden
    seen, out = set(), []
    for f in files:
        if f not in seen:
            out.append(f); seen.add(f)
    return out

def load_all_json_dicts(json_root: str) -> List[Dict[str, Any]]:
    files = find_all_json_files(json_root)
    if not files:
        raise FileNotFoundError(f"No se encontraron JSON en (recursivo): {json_root}")
    print(f"[INFO] JSONs detectados: {len(files)} archivos")

    rows: List[Dict[str, Any]] = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                rows.extend(data)
            elif isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        rows.extend(v)
        except Exception as e:
            print(f"[WARN] No pude leer {f}: {e}")

    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        tabla = str(r.get("tabla", "")).strip()
        variable = str(r.get("variable", "")).strip()
        categorias = r.get("categorias", []) or []
        out.append({"tabla": tabla, "variable": variable, "categorias": categorias})
    print(f"[INFO] Registros de diccionario cargados: {len(out)}")
    return out

def build_base_var_mappings(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Devuelve { base_canon: { variable: { code->label } } }.
    Si hay conflictos entre trimestres, gana el más reciente.
    """
    grouped: Dict[Tuple[str, str], List[Tuple[int, Dict[str, str]]]] = defaultdict(list)

    for r in rows:
        tabla, variable = r["tabla"], r["variable"]
        base, tri, yy = parse_tabla_base(tabla if tabla else "BASE")
        rank = year_quarter_rank(tri, yy)
        cats = r.get("categorias", []) or []

        local_map: Dict[str, str] = {}
        for c in cats:
            code = str(c.get("valor", "")).strip()
            label = str(c.get("categoria", "")).strip()
            if not code or not label:
                continue
            for k in normalize_code_keys(code):
                local_map[k] = label

        grouped[(base, variable)].append((rank, local_map))

    result: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
    for (base, variable), lst in grouped.items():
        lst_sorted = sorted(lst, key=lambda x: x[0])  # del más viejo al más reciente
        merged: Dict[str, str] = {}
        for _, mp in lst_sorted:
            merged.update(mp)  # pisa con el más reciente
        result[base][variable] = merged

    print("[INFO] Bases con mapeos:", ", ".join(sorted(result.keys())))
    return result

# ============================================================
# 3) APLICACIÓN (sin joins) + DIAGNÓSTICO
# ============================================================
BASE_FROM_PARQUET_RE = re.compile(
    r"enoe_master_(coe1t|coe2t|sdemt|hogt|vivt)_labeled\.parquet$", re.IGNORECASE
)

def base_from_parquet_path(path: str) -> str:
    m = BASE_FROM_PARQUET_RE.search(os.path.basename(path))
    if not m:
        raise ValueError(f"No pude inferir la base desde el nombre: {path}")
    key = m.group(1).upper()
    return {"COE1T": "COE1T", "COE2T": "COE2T",
            "SDEMT": "SDEMT", "HOGT": "HOGT", "VIVT": "VIVT"}[key]

def save_columns_list(out_dir: str, parquet_label: str, columns: List[str]) -> None:
    path = os.path.join(out_dir, f"{parquet_label}__columns.txt")
    with open(path, "w", encoding="utf-8") as f:
        for c in columns:
            f.write(c + "\n")

def _make_lookup_udf(mapping: Dict[str, str]):
    """
    Devuelve una función que busca etiquetas con normalización robusta del valor de entrada.
    - str.strip()
    - variante upper para no numéricos
    - numérico: '01' <-> '1' + '02d'
    """
    mapping_keys = set(mapping.keys())

    def norm_variants(v: Any) -> List[str]:
        if v is None:
            return []
        s = str(v).strip()
        out = [s]
        if not re.fullmatch(r"-?\d+", s):
            out.append(s.upper())
        else:
            try:
                n = int(s)
                out.append(str(n))
                if 0 <= n < 100:
                    out.append(f"{n:02d}")
            except ValueError:
                pass
        # quitar duplicados preservando orden
        seen, variants = set(), []
        for k in out:
            if k not in seen:
                variants.append(k); seen.add(k)
        return variants

    def fn(v):
        for k in norm_variants(v):
            if k in mapping_keys:
                return mapping[k]
        return None

    return fn

def apply_labels_to_parquet(
    parquet_path: str,
    base_maps: Dict[str, Dict[str, Dict[str, str]]],
    out_path: str,
    overwrite: bool = True,
    preview_n: int = 5,
    sample_rows_diag: int = 100000
) -> Tuple[int, int, Dict[str, Any]]:
    """
    Aplica mapeos a un parquet sin joins (UDF) y guarda en out_path.
    Retorna:
      (n_cols_mapeadas, n_cols_posibles, diagnostico_dict)
    """
    parquet_label = os.path.basename(parquet_path)

    if (not overwrite) and os.path.exists(out_path):
        raise FileExistsError(f"Ya existe {out_path}")

    base = base_from_parquet_path(parquet_path)
    var_maps = base_maps.get(base, {})
    if not var_maps:
        print(f"[INFO] No hay diccionario consolidado para base {base}. Se copia tal cual.")
        pl.scan_parquet(parquet_path).sink_parquet(out_path)
        schema_cols = pl.read_parquet(parquet_path, n_rows=0).columns
        return 0, 0, {"missing_in_parquet": [], "label_all_null_detail": [], "parquet_columns": schema_cols}

    # ---------- columnas del parquet (y mapa upper->original) ----------
    schema = pl.read_parquet(parquet_path, n_rows=0).schema
    orig_cols = schema.names()
    col_upper_to_orig = {c.upper(): c for c in orig_cols}
    cols_upper = set(col_upper_to_orig.keys())

    # Guardar lista completa de columnas
    parquet_cols_sorted = sorted(orig_cols, key=str.lower)
    save_columns_list(os.path.dirname(out_path), parquet_label, parquet_cols_sorted)
    print(f"[COLS][{parquet_label}] {len(orig_cols)} columnas. (guardado {parquet_label}__columns.txt)")
    print("   " + ", ".join(parquet_cols_sorted[:60]) + (" ..." if len(parquet_cols_sorted) > 60 else ""))

    # ---------- construir expresiones de etiqueta ----------
    exprs: List[pl.Expr] = []
    mapped_pairs: List[Tuple[str, str, Dict[str, str]]] = []  # (var_json_up, parquet_col, mapping)
    missing_in_parquet: List[str] = []
    candidates = 0

    for var_json, mapping in var_maps.items():
        v_up = var_json.upper()
        if v_up in cols_upper and mapping:
            candidates += 1
            parquet_col = col_upper_to_orig[v_up]
            udf = _make_lookup_udf(mapping)
            exprs.append(
                pl.col(parquet_col)
                  .map_elements(udf, return_dtype=pl.Utf8)
                  .alias(f"{parquet_col}_label")
            )
            mapped_pairs.append((v_up, parquet_col, mapping))
        else:
            missing_in_parquet.append(var_json)

    # ---------- aplicar (lazy) y escribir ----------
    lf = pl.scan_parquet(parquet_path)
    if exprs:
        lf = lf.with_columns(exprs)
    lf.sink_parquet(out_path, compression="zstd", statistics=True)

    # ---------- Diagnóstico: *_label 100% nulos + ejemplos ----------
    label_all_null_detail: List[Dict[str, Any]] = []
    try:
        mapped_cols = [pc for _, pc, _ in mapped_pairs]
        if mapped_cols:
            # columnas de salida realmente presentes
            out_cols0 = pl.read_parquet(out_path, n_rows=0).columns
            present_labels = [f"{pc}_label" for pc in mapped_cols if f"{pc}_label" in out_cols0]

            if present_labels:
                sample_labels = pl.read_parquet(out_path, n_rows=sample_rows_diag, columns=present_labels)
                sample_codes  = pl.read_parquet(parquet_path, n_rows=sample_rows_diag, columns=mapped_cols)

                for (v_up, parquet_col, mapping) in mapped_pairs:
                    lab = f"{parquet_col}_label"
                    if lab in sample_labels.columns:
                        nn = sample_labels[lab].drop_nulls().height
                        if nn == 0:
                            # recopilar valores del parquet que no están en el mapping (muestra)
                            try:
                                col_vals = sample_codes[parquet_col].cast(pl.Utf8).drop_nulls()
                                uniq_vals = col_vals.unique().head(50).to_list()
                            except Exception:
                                uniq_vals = []

                            # normalización espejo de la UDF
                            def norm_variants(v: Any) -> List[str]:
                                if v is None:
                                    return []
                                s = str(v).strip()
                                out = [s]
                                if not re.fullmatch(r"-?\d+", s):
                                    out.append(s.upper())
                                else:
                                    try:
                                        n = int(s)
                                        out.append(str(n))
                                        if 0 <= n < 100:
                                            out.append(f"{n:02d}")
                                    except ValueError:
                                        pass
                                seen, vars2 = set(), []
                                for k in out:
                                    if k not in seen:
                                        vars2.append(k); seen.add(k)
                                return vars2

                            mapping_keys = set(mapping.keys())
                            unmapped_samples = []
                            for u in uniq_vals:
                                if not any(k in mapping_keys for k in norm_variants(u)):
                                    unmapped_samples.append(u)
                                if len(unmapped_samples) >= 10:
                                    break

                            sample_map_keys = list(mapping_keys)[:10]

                            label_all_null_detail.append({
                                "var_json": v_up,
                                "parquet_col": parquet_col,
                                "sample_unmapped_codes": unmapped_samples,
                                "sample_mapping_keys": sample_map_keys
                            })
    except Exception:
        pass

    diag = {
        "missing_in_parquet": missing_in_parquet,
        "label_all_null_detail": label_all_null_detail,
        "parquet_columns": parquet_cols_sorted
    }

    # Guardar diagnóstico en JSON junto al parquet de salida
    diag_path = os.path.join(os.path.dirname(out_path), f"{parquet_label}__diagnostic.json")
    try:
        with open(diag_path, "w", encoding="utf-8") as f:
            json.dump(diag, f, ensure_ascii=False, indent=2)
        print(f"[DIAG] Guardado diagnóstico → {os.path.basename(diag_path)}")
    except Exception as e:
        print(f"[WARN] No se pudo guardar diagnóstico: {e}")

    # Preview
    try:
        show = [f"{pc}_label" for _, pc, _ in mapped_pairs][:8]
        out_cols0 = pl.read_parquet(out_path, n_rows=0).columns
        show = [c for c in show if c in out_cols0]
        if show:
            print(f"\n[Preview etiquetas] {os.path.basename(out_path)}")
            print(pl.read_parquet(out_path, n_rows=5, columns=show).to_pandas())
    except Exception:
        pass

    mapped_count = len(mapped_pairs)
    return mapped_count, candidates, diag

# ============================================================
# 4) Descubrir parquets y correr el pipeline
# ============================================================
def discover_parquets(parquet_dir: str) -> List[str]:
    paths = []
    for patt in PARQUET_PATTERNS:
        p = os.path.join(parquet_dir, patt)
        if os.path.exists(p):
            paths.append(p)
    # Quitar duplicados
    seen, out = set(), []
    for f in paths:
        if f not in seen:
            out.append(f); seen.add(f)
    return out

def main():
    json_root = DEFAULT_JSON_ROOT
    in_dir = DEFAULT_PARQUET_DIR
    out_dir = DEFAULT_OUT_DIR

    os.makedirs(out_dir, exist_ok=True)

    print("[INFO] Buscando diccionarios JSON...")
    rows = load_all_json_dicts(json_root)
    base_maps = build_base_var_mappings(rows)

    print("[INFO] Buscando parquets de entrada...")
    parquet_list = discover_parquets(in_dir)
    if not parquet_list:
        print(f"[WARN] No se encontraron parquets en {in_dir}")
        return

    total_mapped = 0
    total_candidates = 0

    for pq in parquet_list:
        out_path = os.path.join(out_dir, os.path.basename(pq))
        parquet_label = os.path.basename(pq)

        print(f"\n[PROC] {parquet_label} → {os.path.basename(out_path)}")
        mapped, candidates, diag = apply_labels_to_parquet(
            parquet_path=pq,
            base_maps=base_maps,
            out_path=out_path,
            overwrite=True
        )

        if diag["parquet_columns"]:
            print(f"[COLS]{parquet_label}: {len(diag['parquet_columns'])} columnas (ver TXT).")

        if diag["missing_in_parquet"]:
            print(f"[DIAG][{parquet_label}] Vars en diccionario pero NO en parquet ({len(diag['missing_in_parquet'])}):")
            print("   " + ", ".join(diag["missing_in_parquet"][:40])
                  + (" ..." if len(diag["missing_in_parquet"]) > 40 else ""))

        if diag["label_all_null_detail"]:
            print(f"[DIAG][{parquet_label}] Vars mapeadas con *_label 100% nulo ({len(diag['label_all_null_detail'])}):")
            for d in diag["label_all_null_detail"][:8]:
                print(f"   var_json={d['var_json']} | parquet_col={d['parquet_col']} | "
                      f"unmapped_examples={d['sample_unmapped_codes']} | "
                      f"map_keys_examples={d['sample_mapping_keys']}")

        print(f"[OK] Etiquetadas: {mapped}/{candidates} columnas posibles.")
        total_mapped += mapped
        total_candidates += candidates

    print(f"\nResumen final: {total_mapped}/{total_candidates} columnas con etiqueta añadida en total.")
    print(f"Salida: {out_dir}")

if __name__ == "__main__":
    main()
