# -*- coding: utf-8 -*-
"""
Copiador de CSVs (aplana estructura):
- Busca recursivamente todos los .csv dentro de 'descompressed'
- Copia cada archivo a 'files' (una sola carpeta)
- Si hay colisiones de nombre, agrega __<subcarpeta> y un contador incremental
"""

from pathlib import Path
import shutil
import logging
import re

# === RUTAS (ajústalas si cambian) ===
SRC_ROOT = Path(r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\descompressed")
DST_DIR  = Path(r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\files")

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

def sanitize(name: str) -> str:
    """Deja el nombre seguro para usar en archivo (sin caracteres raros)."""
    name = name.strip().replace(" ", "_")
    return re.sub(r'[^A-Za-z0-9_\-\.]+', "_", name)

def unique_dest_path(base_dir: Path, stem: str, suffix: str, tag: str) -> Path:
    """Genera una ruta de destino única evitando choques."""
    candidate = base_dir / f"{stem}__{tag}{suffix}"
    if not candidate.exists():
        return candidate
    i = 2
    while True:
        cand = base_dir / f"{stem}__{tag}__{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1

def main():
    if not SRC_ROOT.exists():
        logging.error("No existe el directorio de origen: %s", SRC_ROOT)
        return
    DST_DIR.mkdir(parents=True, exist_ok=True)

    # Recorre recursivamente todos los .csv (cualquier mayúscula/minúscula)
    csv_files = [p for p in SRC_ROOT.rglob("*") if p.is_file() and p.suffix.lower() == ".csv"]
    if not csv_files:
        logging.warning("No se encontraron .csv dentro de: %s", SRC_ROOT)
        return

    logging.info("CSVs encontrados: %d", len(csv_files))
    copiados = 0

    for src in csv_files:
        try:
            # Subcarpeta "top-level" relativa a SRC_ROOT (para etiquetar el origen)
            rel = src.relative_to(SRC_ROOT)
            top = rel.parts[0] if len(rel.parts) > 1 else "root"
            tag = sanitize(top)

            # Construir nombre destino único
            stem, suffix = src.stem, src.suffix  # e.g., .csv
            dest = unique_dest_path(DST_DIR, sanitize(stem), suffix, tag)

            shutil.copy2(src, dest)  # si prefieres mover: shutil.move(src, dest)
            copiados += 1
            logging.info("Copiado: %s  →  %s", src, dest)
        except Exception as e:
            logging.error("Error copiando %s: %s", src, e)

    logging.info("Proceso terminado. Archivos copiados: %d. Destino: %s", copiados, DST_DIR)

if __name__ == "__main__":
    main()
