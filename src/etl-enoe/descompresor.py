# -*- coding: utf-8 -*-
"""
Descompresor ENOE (ZIP → carpetas)
- Recorre todos los .zip dentro de la carpeta 'comprimidos'
- Extrae cada ZIP en una subcarpeta dentro de 'descompressed' con el mismo nombre del ZIP
- Incluye protección contra 'zip slip' (paths maliciosos) y logging claro

Requisitos: Python 3.9+ (probado en 3.12). Sin librerías externas.
"""

from pathlib import Path
import zipfile
import logging

# === RUTAS (ajustadas a tu caso) ===
INPUT_DIR = Path(r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\comprimidos")
OUTPUT_DIR = Path(r"C:\Users\betoh\OneDrive\Escritorio\Yo\Economía\7mo Semestre\hackaton inegi\microdatos-enoe\descompressed")

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)

def safe_extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """
    Extrae un archivo ZIP en dest_dir, evitando path traversal.
    Crea directorios necesarios automáticamente.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, mode="r") as zf:
        for info in zf.infolist():
            # Ruta de destino propuesta (se respeta la estructura interna del ZIP)
            target_path = (dest_dir / info.filename).resolve()

            # Protección: el archivo a extraer DEBE quedar dentro de dest_dir
            if not str(target_path).startswith(str(dest_dir.resolve())):
                logging.warning("Omitido por path traversal: %s dentro de %s", info.filename, zip_path.name)
                continue

            if info.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            # Asegurar carpeta y escribir el archivo
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(target_path, "wb") as dst:
                dst.write(src.read())

def main():
    if not INPUT_DIR.exists():
        logging.error("No existe la carpeta de entrada: %s", INPUT_DIR)
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    zip_files = [p for p in INPUT_DIR.iterdir() if p.is_file() and zipfile.is_zipfile(p)]
    if not zip_files:
        logging.warning("No se encontraron archivos ZIP en: %s", INPUT_DIR)
        return

    logging.info("ZIPs encontrados: %d", len(zip_files))

    for zip_path in zip_files:
        # Subcarpeta destino: nombre del ZIP sin extensión
        dest_subdir = OUTPUT_DIR / zip_path.stem
        logging.info("Extrayendo: %s  →  %s", zip_path.name, dest_subdir)
        try:
            safe_extract_zip(zip_path, dest_subdir)
        except Exception as e:
            logging.error("Error al extraer %s: %s", zip_path.name, e)

    logging.info("Proceso terminado. Archivos extraídos en: %s", OUTPUT_DIR)

if __name__ == "__main__":
    main()
