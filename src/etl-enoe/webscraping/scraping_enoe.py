# -*- coding: utf-8 -*-
"""
Scraper RNM INEGI (ENOE 2024) – Diccionario + Detalle de variables

Cambios (2025-11-05):
- FIX: 'Visión general' ahora se extrae desde la TABLA (Válido/No válido/Tipo/Decimal/Ancho/Rango/Formato).
- FIX: 'Pregunta literal' detectada dentro de 'Preguntas e instrucciones' (strong/div/span), no solo h2/h3/h4.
- FIX: 'Categorías' robusto: busca encabezado y, de no hallarlo, cualquier tabla con ['Valor','Categoría'].
- Mantiene MODO PRUEBA (1 tabla, N variables) y CLI (--test, --table, --file-id, --limit-vars).

Requisitos: pip install requests beautifulsoup4 lxml pandas
"""

import re
import time
import json
import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
from typing import Optional, Dict, List, Tuple

BASE = "https://www.inegi.org.mx/rnm/index.php"
CATALOG_ID = 1016  # ENOE 2024

# ====== Config rápida para pruebas ======
TEST_MODE = False
N_PREVIEW_VARS = 80
SAVE_DIR_PREFIX = ""
PAUSE_SEC = 0.35

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _get(url: str) -> requests.Response:
    for _ in range(3):
        r = SESSION.get(url, timeout=30)
        if r.status_code == 200:
            return r
        time.sleep(1.5)
    r.raise_for_status()


# ---------- Descubrimiento de F-id (robusto) ----------
def _discover_file_id_via_redirect(catalog_id: int, file_name: str) -> Optional[str]:
    url = f"{BASE}/catalog/{catalog_id}/data-dictionary?file_name={file_name}"
    r = _get(url)
    m = re.search(r"/data-dictionary/(F\d+)\b", r.url)
    if m:
        return m.group(1)
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select('a[href*="/data-dictionary/"]'):
        href = urljoin(BASE, a.get("href", ""))
        if f"file_name={file_name}" in href:
            m2 = re.search(r"/data-dictionary/(F\d+)\b", href)
            if m2:
                return m2.group(1)
    return None


def _discover_file_id_via_catalog_home(catalog_id: int, file_name: str) -> Optional[str]:
    url = f"{BASE}/catalog/{catalog_id}"
    r = _get(url)
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select('a[href*="/data-dictionary/"]'):
        txt = (a.get_text(strip=True) or "")
        href = a.get("href", "")
        if txt == file_name:
            m = re.search(r"/data-dictionary/(F\d+)\?file_name=", href)
            if m:
                return m.group(1)
    for a in soup.select('a[href*="/data-dictionary/"]'):
        href = a.get("href", "")
        if f"file_name={file_name}" in href:
            m = re.search(r"/data-dictionary/(F\d+)\?file_name=", href)
            if m:
                return m.group(1)
    return None


def discover_file_id_by_name(catalog_id: int, file_name: str) -> str:
    f_id = _discover_file_id_via_redirect(catalog_id, file_name)
    if f_id:
        print(f"[resolver] F-id por redirección: {file_name} -> {f_id}")
        return f_id
    f_id = _discover_file_id_via_catalog_home(catalog_id, file_name)
    if f_id:
        print(f"[resolver] F-id por portada catálogo: {file_name} -> {f_id}")
        return f_id
    raise ValueError(
        f"No pude encontrar el F-id para {file_name}. "
        f"Usa --file-id Fxx o define FILE_IDS['{file_name}']='Fxx'."
    )


# ---------- Listado de variables ----------
def list_variables(catalog_id: int, file_id: str, file_name: str) -> List[Tuple[str, str]]:
    url = f"{BASE}/catalog/{catalog_id}/data-dictionary/{file_id}?file_name={file_name}"
    r = _get(url)
    soup = BeautifulSoup(r.text, "lxml")
    var_map: Dict[str, str] = {}
    for a in soup.select('a[href*="/variable/"]'):
        href = a.get("href", "")
        if "name=" in href and "/variable/" in href:
            full = urljoin(BASE, href)
            q = parse_qs(urlparse(full).query)
            name = (q.get("name", [""])[0] or "").strip()
            if name:
                var_map[name] = full
    items = [(k, var_map[k]) for k in sorted(var_map.keys())]
    if not items:
        print(f"[WARN] No encontré variables para {file_name} ({file_id}).")
    return items


# ---------- Helpers de parseo ----------
def _normalize_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s


def _find_header(soup: BeautifulSoup, keywords: List[str]) -> Optional[BeautifulSoup]:
    # Busca h2/h3/h4 cuyo texto contenga alguna palabra clave (tolerante a acentos)
    patt = re.compile("|".join([re.escape(k) for k in keywords]), re.IGNORECASE)
    for tag in soup.find_all(["h2", "h3", "h4"]):
        if patt.search(tag.get_text(" ", strip=True)):
            return tag
    return None


def parse_overview_table(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Extrae la tabla de 'Visión general' como dict:
    {'Válido': '...', 'No válido': '...', 'Tipo': '...', 'Decimal': '...',
     'Ancho': '...', 'Rango': '...', 'Formato': '...'}
    """
    out = {"Válido": "", "No válido": "", "Tipo": "", "Decimal": "",
           "Ancho": "", "Rango": "", "Formato": ""}

    hdr = _find_header(soup, ["Visión general", "Vision general"])
    table = None
    if hdr:
        table = hdr.find_next("table")
    if not table:
        # Fallback: cualquier tabla que tenga filas con esas etiquetas
        for t in soup.find_all("table"):
            head_text = t.get_text(" ", strip=True)
            if any(lbl in head_text for lbl in ["Válido", "No válido", "Tipo", "Decimal", "Ancho", "Rango", "Formato"]):
                table = t
                break

    if not table:
        return out

    # Lee filas tipo: <tr><td>Etiqueta</td><td>Valor</td>...</tr>
    for tr in table.select("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) >= 2:
            key = _normalize_text(tds[0])
            val = _normalize_text(tds[1])
            # Normaliza claves posibles
            if re.search(r"^válido$", key, re.IGNORECASE):
                out["Válido"] = val
            elif re.search(r"^no\s+válido$", key, re.IGNORECASE):
                out["No válido"] = val
            elif re.search(r"^tipo$", key, re.IGNORECASE):
                out["Tipo"] = val
            elif re.search(r"^decimal(es)?$", key, re.IGNORECASE):
                out["Decimal"] = val
            elif re.search(r"^ancho$", key, re.IGNORECASE):
                out["Ancho"] = val
            elif re.search(r"^rango$", key, re.IGNORECASE):
                out["Rango"] = val
            elif re.search(r"^formato$", key, re.IGNORECASE):
                out["Formato"] = val
    return out


def parse_question_literal(soup: BeautifulSoup) -> str:
    """
    Obtiene el texto de 'Pregunta literal' dentro de 'Preguntas e instrucciones'.
    Estrategia:
    1) Encuentra un nodo con texto 'Pregunta literal' (strong/div/span/td).
    2) Toma el siguiente hermano/bloque textual significativo como la pregunta.
    """
    # Busca contenedor de 'Preguntas e instrucciones' (opcional)
    cont = _find_header(soup, ["Preguntas e instrucciones"])
    search_root = cont if cont else soup

    # Busca etiqueta 'Pregunta literal' (tolerante)
    label_node = search_root.find(
        lambda tag: tag.name in ("strong", "b", "div", "span", "td") and
        re.search(r"pregunta\s+literal", tag.get_text(" ", strip=True), re.IGNORECASE)
    )
    if not label_node:
        # Fallback: busca por texto directo
        label_node = search_root.find(string=re.compile(r"^\s*Pregunta\s+literal\s*:?\s*$", re.IGNORECASE))
        if label_node:
            label_node = label_node.parent

    if not label_node:
        return ""

    # Toma el siguiente elemento con texto no vacío que NO sea otro label/sección
    for el in label_node.next_elements:
        if getattr(el, "name", None) in ("h2", "h3", "h4"):
            break
        if hasattr(el, "get_text"):
            txt = _normalize_text(el.get_text(" ", strip=True))
        else:
            txt = _normalize_text(str(el))
        if not txt:
            continue
        # Evita repetir el propio rótulo
        if re.search(r"^pregunta\s+literal", txt, re.IGNORECASE):
            continue
        # Corta si topa otra subsección conocida
        if re.search(r"^(categorías|categories|contexto|descripción|definición|universo|source|fuente)\b",
                     txt, re.IGNORECASE):
            break
        # Ese primer texto sustantivo es la pregunta
        return txt
    return ""


def parse_categories(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Extrae la tabla de 'Categorías' (o 'Categories'). Devuelve lista de dicts:
    [{'valor','categoria','casos','porcentaje'}], filtrando encabezados y 'Sysmiss'.
    """
    cats: List[Dict[str, str]] = []

    # 1) Busca encabezado
    hdr = _find_header(soup, ["Categorías", "Categories"])
    table = hdr.find_next("table") if hdr else None

    # 2) Fallback: busca cualquier tabla con cabecera 'Valor' y 'Categoría'
    if not table:
        for t in soup.find_all("table"):
            ths = [th.get_text(" ", strip=True).lower() for th in t.select("th")]
            tds_first = [td.get_text(" ", strip=True).lower() for td in t.select("tr:first-child td")]
            header_cells = ths or tds_first
            if any("valor" in c for c in header_cells) and any("categor" in c for c in header_cells):
                table = t
                break

    if not table:
        return cats

    for tr in table.select("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) >= 2:
            valor, categoria = tds[0], tds[1]
            if valor.lower() in ("valor", "sysmiss"):
                continue
            casos = tds[2] if len(tds) >= 3 else ""
            porcentaje = tds[3] if len(tds) >= 4 else ""
            cats.append({
                "valor": valor,
                "categoria": categoria,
                "casos": casos,
                "porcentaje": porcentaje
            })
    return cats


# ---------- Parse principal de variable ----------
def parse_variable_detail(var_url: str) -> Dict[str, object]:
    r = _get(var_url)
    soup = BeautifulSoup(r.text, "lxml")

    # Etiqueta (título)
    etiqueta = ""
    title_h2 = soup.find("h2")
    if title_h2:
        title = title_h2.get_text(" ", strip=True)
        m = re.match(r"^(.*)\(([^)]+)\)\s*$", title)
        etiqueta = (m.group(1).strip() if m else title.strip())

    # Visión general desde TABLA
    vision = parse_overview_table(soup)
    valido = vision.get("Válido", "")
    no_valido = vision.get("No válido", "")
    tipo = vision.get("Tipo", "")
    decimales = vision.get("Decimal", "")
    ancho = vision.get("Ancho", "")
    rango = vision.get("Rango", "")
    formato = vision.get("Formato", "")

    # Pregunta literal robusta
    pregunta_literal = parse_question_literal(soup)

    # Textos de secciones (por si están luego)
    def _grab_section_text(header_text: str) -> str:
        def _match_hdr(tag):
            if tag.name not in ("h2", "h3", "h4"):
                return False
            return header_text.lower() in tag.get_text(strip=True).lower()

        hdr = soup.find(_match_hdr)
        if not hdr:
            strong = soup.find("strong", string=re.compile(header_text, re.IGNORECASE))
            hdr = strong.parent if strong else None
        if not hdr:
            return ""
        texts = []
        for sib in hdr.next_siblings:
            if getattr(sib, "name", None) in ("h2", "h3", "h4"):
                break
            if getattr(sib, "name", None) in (None, "p", "div", "span"):
                t = (sib.get_text(" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip())
                if t:
                    texts.append(t)
        return _normalize_text(" ".join(texts))

    definicion = _grab_section_text("Definición") or _grab_section_text("Definition")
    universo = _grab_section_text("Universo") or _grab_section_text("Universe")
    fuente = (_grab_section_text("Source of information") or
              _grab_section_text("Fuente de información") or
              _grab_section_text("Fuente"))

    categorias = parse_categories(soup)
    categorias_json = json.dumps(categorias, ensure_ascii=False)

    return {
        "etiqueta": etiqueta,
        "pregunta": pregunta_literal,
        "valido": valido,
        "no_valido": no_valido,
        "tipo": tipo,
        "decimales": decimales,
        "ancho": ancho,
        "rango": rango,
        "formato": formato,
        "definicion": definicion,
        "universo": universo,
        "fuente": fuente,
        "categorias": categorias,
        "categorias_json": categorias_json,
        "url_variable": var_url,
    }


# ---------- Wrappers de scrape ----------
def scrape_single_table(catalog_id: int,
                        file_name: str,
                        file_id: Optional[str] = None,
                        pause_sec: float = 0.35,
                        limit_vars: Optional[int] = None,
                        strict_errors: bool = True,
                        verbose: bool = True) -> pd.DataFrame:
    f_id = file_id or discover_file_id_by_name(catalog_id, file_name)
    if verbose:
        print(f"[INFO] Tabla={file_name} ⇒ F-id={f_id}")

    var_list = list_variables(catalog_id, f_id, file_name)
    if limit_vars is not None:
        var_list = var_list[:max(0, int(limit_vars))]

    rows = []
    for idx, (var_name, var_url) in enumerate(var_list, 1):
        if verbose:
            print(f"[{file_name}] Variable {idx}/{len(var_list)}: {var_name}")
        try:
            detail = parse_variable_detail(var_url)
            row = {"tabla": file_name, "variable": var_name, **detail, "error": ""}
            rows.append(row)
        except Exception as e:
            if strict_errors:
                raise
            else:
                rows.append({
                    "tabla": file_name,
                    "variable": var_name,
                    "etiqueta": "", "pregunta": "",
                    "valido": "", "no_valido": "", "tipo": "", "decimales": "",
                    "ancho": "", "rango": "", "formato": "",
                    "definicion": "", "universo": "", "fuente": "",
                    "categorias": [], "categorias_json": "[]",
                    "url_variable": var_url,
                    "error": f"{type(e).__name__}: {e}"
                })
        time.sleep(pause_sec)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    ordered = ["tabla","variable","etiqueta","pregunta",
               "valido","no_valido","tipo","decimales","ancho","rango","formato",
               "definicion","universo","fuente","categorias","categorias_json",
               "url_variable","error"]
    for c in ordered:
        if c not in df.columns:
            df[c] = ""
    return df[ordered]


def scrape_tables(catalog_id: int,
                  table_names: List[str],
                  file_ids: Optional[Dict[str, str]] = None,
                  pause_sec: float = 0.4,
                  max_tables: Optional[int] = None,
                  max_vars_per_table: Optional[int] = None,
                  save_intermediate: bool = True) -> pd.DataFrame:
    rows_all: List[Dict[str, object]] = []
    file_ids = file_ids or {}

    if max_tables is not None:
        table_names = table_names[:max(0, int(max_tables))]

    for t_idx, file_name in enumerate(table_names, 1):
        print(f"\n==> Procesando tabla {t_idx}/{len(table_names)}: {file_name}")
        f_id = file_ids.get(file_name) or discover_file_id_by_name(catalog_id, file_name)
        print(f"[INFO] {file_name} ⇒ F-id={f_id}")
        var_list = list_variables(catalog_id, f_id, file_name)
        if max_vars_per_table is not None:
            var_list = var_list[:max(0, int(max_vars_per_table))]

        rows_table: List[Dict[str, object]] = []
        for v_idx, (var_name, var_url) in enumerate(var_list, 1):
            print(f"   - {var_name} ({v_idx}/{len(var_list)})")
            try:
                detail = parse_variable_detail(var_url)
                rows_table.append({"tabla": file_name, "variable": var_name, **detail, "error": ""})
            except Exception as e:
                rows_table.append({
                    "tabla": file_name,
                    "variable": var_name,
                    "etiqueta": "", "pregunta": "",
                    "valido": "", "no_valido": "", "tipo": "", "decimales": "",
                    "ancho": "", "rango": "", "formato": "",
                    "definicion": "", "universo": "", "fuente": "",
                    "categorias": [], "categorias_json": "[]",
                    "url_variable": var_url,
                    "error": f"{type(e).__name__}: {e}"
                })
            time.sleep(pause_sec)

        df_t = pd.DataFrame(rows_table)
        ordered = ["tabla","variable","etiqueta","pregunta",
                   "valido","no_valido","tipo","decimales","ancho","rango","formato",
                   "definicion","universo","fuente","categorias","categorias_json",
                   "url_variable","error"]
        for c in ordered:
            if c not in df_t.columns:
                df_t[c] = ""
        df_t = df_t[ordered]

        if save_intermediate:
            out_csv = f"{SAVE_DIR_PREFIX}rnm_diccionario_{file_name}.csv"
            out_json = f"{SAVE_DIR_PREFIX}rnm_diccionario_{file_name}.json"
            _save_df_with_categories(df_t, out_csv, out_json)
            print(f"   Guardado intermedio -> {out_csv} / {out_json}")

        rows_all.extend(df_t.to_dict(orient="records"))

    df_all = pd.DataFrame(rows_all)
    if df_all.empty:
        return df_all

    final_cols = ["tabla","variable","etiqueta","pregunta",
                  "valido","no_valido","tipo","decimales","ancho","rango","formato",
                  "definicion","universo","fuente","categorias","categorias_json",
                  "url_variable","error"]
    for c in final_cols:
        if c not in df_all.columns:
            df_all[c] = ""
    return df_all[final_cols]


# ---------- Guardado ----------
def _save_df_with_categories(df: pd.DataFrame, out_csv: str, out_json: str) -> None:
    df_for_json = df.copy()
    df_for_json["categorias"] = df_for_json["categorias"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(df_for_json.to_dict(orient="records"), f, ensure_ascii=False, indent=2)


# ---------- CLI ----------
def _cli_args():
    p = argparse.ArgumentParser(description="Scraper RNM INEGI – Diccionario de variables")
    p.add_argument("--catalog-id", type=int, default=CATALOG_ID)
    p.add_argument("--table", type=str, help="Nombre de tabla (ej. COE2T324)")
    p.add_argument("--file-id", type=str, help="Forzar F-id (ej. F35)")
    p.add_argument("--test", action="store_true", help="Modo prueba (solo 1 tabla)")
    p.add_argument("--limit-vars", type=int, default=None, help="Limitar # de variables")
    p.add_argument("--pause", type=float, default=PAUSE_SEC)
    p.add_argument("--full", action="store_true", help="Ignora TEST_MODE y corre todo")
    return p.parse_args()


# ---------- Main ----------
if __name__ == "__main__":
    TABLAS = ["COE2T324","COE1T324","SDEMT324","HOGT324","VIVT324","COE2T224","COE1T224","SDEMT224","HOGT224","VIVT224","HOGT424","VIVT424","SDEMT424","COE1T424","COE2T424"]
    FILE_IDS = {
        # "COE2T324": "F35",  # Descomenta si quieres forzar
    }

    args = _cli_args()
    run_test = TEST_MODE or args.test
    if args.full:
        run_test = False

    pause = args.pause
    catalog = args.catalog_id

    if run_test:
        table = args.table or (TABLAS[0] if TABLAS else None)
        if not table:
            raise SystemExit("No hay tabla definida para prueba. Usa --table COE2T324 o define TABLAS.")
        limit_vars = args.limit_vars if args.limit_vars is not None else N_PREVIEW_VARS
        forced_fid = args.file_id or FILE_IDS.get(table)

        print(f"[MODO PRUEBA] Catálogo={catalog} | Tabla={table} | limit_vars={limit_vars} | file_id={forced_fid or 'auto'}")
        df = scrape_single_table(
            catalog_id=catalog,
            file_name=table,
            file_id=forced_fid,
            pause_sec=pause,
            limit_vars=limit_vars,
            strict_errors=True,
            verbose=True
        )

        out_csv = f"{SAVE_DIR_PREFIX}TEST_diccionario_{table}.csv"
        out_json = f"{SAVE_DIR_PREFIX}TEST_diccionario_{table}.json"
        _save_df_with_categories(df, out_csv, out_json)

        print("\nVista rápida (head):")
        with pd.option_context("display.max_colwidth", 120, "display.width", 180):
            print(df.head(min(10, len(df))))
        print(f"\nListo (modo prueba). Filas: {len(df)}")
        print(f"CSV:  {out_csv}")
        print(f"JSON: {out_json}")

    else:
        print(f"[MODO COMPLETO] Catálogo={catalog} | Tablas={len(TABLAS)}")
        df_all = scrape_tables(
            catalog_id=catalog,
            table_names=TABLAS,
            file_ids=FILE_IDS,
            pause_sec=pause,
            max_tables=None,
            max_vars_per_table=None,
            save_intermediate=True
        )
        if df_all.empty:
            print("No se obtuvieron filas.")
        else:
            out_csv = f"{SAVE_DIR_PREFIX}rnm_diccionario_completo_ALL.csv"
            out_json = f"{SAVE_DIR_PREFIX}rnm_diccionario_completo_ALL.json"
            _save_df_with_categories(df_all, out_csv, out_json)
            print(f"\nListo (modo completo). Filas totales: {len(df_all)}")
            print(f"CSV final:  {out_csv}")
            print(f"JSON final: {out_json}")
