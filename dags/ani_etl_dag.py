# dags/ani_etl_dag.py

import pandas as pd
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException

# Importar funciones de los módulos
from extraction.scraper import extract_data
from validation.validator import validate_data, load_rules
from persistence.writer import write_data, DatabaseManager

log = logging.getLogger(__name__)

# --- CONFIGURACIÓN DEL DAG (Leída desde .env) ---
DAG_ID = os.environ.get("AIRFLOW_DAG_ID", "ani_etl_pipeline")
DAG_SCHEDULE = os.environ.get("AIRFLOW_DAG_SCHEDULE", "@daily")
ENTITY_VALUE = os.environ.get("ANI_ENTITY_VALUE", "Agencia Nacional de Infraestructura")
VALIDATION_CONFIG_PATH = os.environ.get("VALIDATION_CONFIG_PATH", "/opt/airflow/configs/validation_rules.yml")
TEMP_DATA_DIR = "/tmp"


# --- FIN DE CONFIGURACIÓN ---

# --- Funciones Helper para manejo de archivos ---
def get_temp_filepath(task_id: str, run_id: str) -> str:
    """Genera una ruta de archivo temporal única por ejecución."""
    filename = f"{task_id}_{run_id.replace(':', '_')}.parquet"
    return os.path.join(TEMP_DATA_DIR, filename)


def cleanup_temp_files(filepaths: List[str]):
    """Elimina los archivos temporales."""
    for fp in filepaths:
        try:
            if os.path.exists(fp):
                os.remove(fp)
                log.info(f"Archivo temporal eliminado: {fp}")
        except Exception as e:
            log.warning(f"No se pudo eliminar el archivo temporal {fp}: {e}")


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=['etl', 'scraping', 'prueba-tecnica-final'],
    doc_md="""
    Pipeline ETL para normatividad ANI[cite: 6].
    Implementa un flujo de 4 tareas:
    1.  **Asegurar Esquema**: (Idempotente) Crea las tablas si no existen.
    2.  **Extracción**: Scrapea el sitio de ANI[cite: 19].
    3.  **Validación**: Limpia y valida los datos contra un archivo YAML[cite: 23].
    4.  **Escritura**: Inserta los datos válidos en Postgres[cite: 20].
    """
)
def ani_etl_pipeline():
    @task(task_id="ensure_schema")
    def task_ensure_schema():
        """Tarea de preparación: Ejecuta el DDL (CREATE TABLE IF NOT EXISTS)."""
        log.info("Iniciando tarea: Asegurar Esquema de BD...")
        db_manager = DatabaseManager()
        try:
            if not db_manager.connect():
                raise Exception("Fallo al conectar con la BD para asegurar esquema.")
            db_manager.ensure_schema_exists()
        except Exception as e:
            log.error(f"Fallo en la tarea 'ensure_schema': {e}")
            raise
        finally:
            if db_manager:
                db_manager.close()
        log.info("Esquema asegurado exitosamente.")

    @task(task_id="extract_data")
    def task_extract(**kwargs) -> str:
        """Tarea de Extracción: Scrapea datos y los guarda en un archivo Parquet."""
        run_id = kwargs['dag_run'].run_id
        num_pages = kwargs['params'].get('num_pages')
        log.info(f"Iniciando tarea de extracción para {num_pages} páginas.")

        df = extract_data(num_pages_to_scrape=num_pages)

        # Log de totales extraídos
        log.info(f"--- TOTALES EXTRAÍDOS: {len(df)} ---")

        if df.empty:
            raise AirflowSkipException("No data extracted.")

        filepath = get_temp_filepath("extracted", run_id)
        df.to_parquet(filepath, index=False)
        log.info(f"Datos extraídos guardados en: {filepath}")
        return filepath

    @task(task_id="validate_data")
    def task_validate(extracted_filepath: str, **kwargs) -> str:
        """Tarea de Validación: Lee, valida según reglas[cite: 26], y guarda en un nuevo archivo."""
        run_id = kwargs['dag_run'].run_id
        log.info(f"Iniciando tarea de validación desde el archivo: {extracted_filepath}")

        df = pd.read_parquet(extracted_filepath)
        rules = load_rules(VALIDATION_CONFIG_PATH)
        validated_df = validate_data(df, rules)

        # Log de descartes
        descartes = len(df) - len(validated_df)
        log.info(f"--- DESCARTES POR VALIDACIÓN: {descartes} ---")

        if validated_df.empty:
            raise AirflowSkipException("No valid data left after validation.")

        validated_filepath = get_temp_filepath("validated", run_id)
        validated_df.to_parquet(validated_filepath, index=False)
        log.info(f"Datos validados guardados en: {validated_filepath}")
        return validated_filepath

    @task(task_id="write_data")
    def task_write(validated_filepath: str, extracted_filepath: str) -> Dict[str, Any]:
        """Tarea de Escritura: Inserta en BD (con idempotencia)  y limpia archivos."""
        log.info(f"Iniciando tarea de escritura desde el archivo: {validated_filepath}")

        df = pd.read_parquet(validated_filepath)
        result = write_data(df, entity=ENTITY_VALUE)

        # Log de filas insertadas
        log.info(f"--- FILAS INSERTADAS: {result.get('records_inserted', 0)} ---")

        log.info("Limpiando archivos temporales...")
        cleanup_temp_files([validated_filepath, extracted_filepath])

        return result

    # --- Definición de la Secuencia del DAG [cite: 29] ---
    schema_task = task_ensure_schema()
    extracted_fp = task_extract()
    validated_fp = task_validate(extracted_fp)
    write_summary = task_write(validated_fp, extracted_fp)

    # El flujo de datos (E-V-W) depende de que el esquema esté listo.
    schema_task >> extracted_fp >> validated_fp >> write_summary


# Instanciar el DAG
ani_etl_pipeline()