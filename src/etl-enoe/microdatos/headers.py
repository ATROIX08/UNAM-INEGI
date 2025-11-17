# -*- coding: utf-8 -*-
"""
Agrupador de CSVs por headers (ENOE)
------------------------------------
- Lee los headers de cada .csv en la ruta indicada (recursivo).
- Agrupa por headers *normalizados* (minúsculas, espacios contraídos, sin BOM).
- Imprime:
    * Grupos con el mismo header (cuántos y cuáles archivos)
    * CSVs con headers únicos (solo un archivo por header)
- Opcional: guarda un resumen en CSV y un reporte en TXT.

Requisitos: solo librerías estándar de Python.
"""

from pathlib import Path
from collections import defaultdict
import csv
import io
import logging
import re

# === RUTAS (ajústalas si cambian) ===
CSV_DIR = Path(r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\files")

# === OPCIONES ===
RECURSIVO = True                 # Si True, busca con rglob; si False, solo en el directorio inmediato
WRITE_SUMMARY_FILES = True       # Guardar 'headers_groups_summary.csv' y 'headers_groups_report.txt'
SUMMARY_BASENAME = "headers_groups"

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# ---------- Utilidades ----------

def _normalize_header_name(s: str) -> str:
    """Normaliza un nombre de columna: minúsculas, sin BOM, espacios contraídos."""
    if s is None:
        return ""
    s = s.replace("\ufeff", "")          # quita BOM si lo hay
    s = s.strip()
    s = re.sub(r"\s+", " ", s)          # contrae espacios múltiples
    return s.lower()

def _best_decode(b: bytes) -> str:
    """Decodifica bytes a texto intentando utf-8-sig y latin-1 (fallback)."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    # Último recurso (no debería ocurrir):
    return b.decode("utf-8", errors="replace")

def _sniff_delimiter(sample_text: str, default=",") -> str:
    """Intenta detectar el delimitador; si falla, devuelve default (',')."""
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",", ";", "|", "\t"])
        return dialect.delimiter
    except Exception:
        return default

def read_csv_header(path: Path):
    """
    Devuelve (header_original:list[str], header_normalizado:tuple[str], delimiter:str)
    Si el archivo está vacío o no hay filas, devuelve ([], (), delim).
    """
    # Leemos un bloque pequeño para olfatear (sniff) el delimitador
    with open(path, "rb") as f:
        chunk = f.read(64 * 1024)  # 64KB
    text = _best_decode(chunk)

    delim = _sniff_delimiter(text)
    # Usamos solo la primera línea completa. Si el sniff no capturó salto, usamos el lector.
    sio = io.StringIO(text)
    reader = csv.reader(sio, delimiter=delim)
    try:
        row = next(reader, [])
    except Exception:
        row = []

    # Si por alguna razón la primera línea salió vacía, intentamos abrir completo
    if not row:
        try:
            with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
                reader2 = csv.reader(f, delimiter=delim)
                row = next(reader2, [])
        except Exception:
            row = []

    header_original = [c.strip() for c in row]
    header_normalizado = tuple(_normalize_header_name(c) for c in header_original)
    return header_original, header_normalizado, delim

# ---------- Proceso principal ----------

def main():
    if not CSV_DIR.exists():
        logging.error("No existe el directorio: %s", CSV_DIR)
        return

    iterator = CSV_DIR.rglob("*.csv") if RECURSIVO else CSV_DIR.glob("*.csv")
    csv_files = [p for p in iterator if p.is_file()]
    if not csv_files:
        logging.warning("No se encontraron archivos .csv en %s", CSV_DIR)
        return

    logging.info("CSVs encontrados: %d", len(csv_files))

    groups = defaultdict(list)     # key = header_normalizado  -> list[Path]
    key_to_sample_header = {}      # key -> header_original (del primer archivo del grupo)
    key_to_delimiter = {}          # key -> delimitador detectado
    errores = []

    for p in sorted(csv_files):
        try:
            hdr_orig, hdr_norm, delim = read_csv_header(p)
            groups[hdr_norm].append(p)
            key_to_delimiter.setdefault(hdr_norm, delim)
            # Guardar un ejemplo legible del header original (la 1ª vez que vemos el grupo)
            if hdr_norm not in key_to_sample_header:
                key_to_sample_header[hdr_norm] = hdr_orig
        except Exception as e:
            errores.append((p, str(e)))

    # --- Salida en consola en el formato solicitado ---
    # 1) Grupos con más de un archivo
    print("\n================ GRUPOS (mismo header) ================\n")
    multi = [(k, v) for k, v in groups.items() if len(v) > 1]
    # Ordenamos por tamaño de grupo (desc) y por nombre de header para estabilidad
    multi.sort(key=lambda kv: (-len(kv[1]), " | ".join(key_to_sample_header.get(kv[0], kv[0]))))

    if not multi:
        print("No hay grupos con múltiples archivos. Todos los CSVs parecen tener headers distintos.\n")
    else:
        for idx, (key, files) in enumerate(multi, start=1):
            header_legible = " | ".join(key_to_sample_header.get(key, [])) or "(sin encabezados)"
            print(f"({idx}) csvs con los headers:\n{header_legible}\n")
            for f in files:
                print(f"  - {f.name}")
            print(f"\nTotal: {len(files)} archivos. Delimitador detectado: '{key_to_delimiter.get(key, ',')}'\n")

    # 2) Headers únicos
    print("\n================ HEADERS ÚNICOS (1 archivo) ================\n")
    singles = [(k, v[0]) for k, v in groups.items() if len(v) == 1]
    if not singles:
        print("No hay headers únicos (todos pertenecen a algún grupo con >1 archivo).\n")
    else:
        # Orden alfabético por header
        singles.sort(key=lambda kv: " | ".join(key_to_sample_header.get(kv[0], [])))
        for k, file_path in singles:
            header_legible = " | ".join(key_to_sample_header.get(k, [])) or "(sin encabezados)"
            print("csv con los headers:")
            print(header_legible)
            print(f"  - {file_path.name}\n")

    # 3) Errores (si los hubo)
    if errores:
        print("\n================ ARCHIVOS CON ERROR ==================\n")
        for p, msg in errores:
            print(f"- {p.name}: {msg}")

if __name__ == "__main__":
    main()
