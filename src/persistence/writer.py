import pandas as pd
import psycopg2
import psycopg2.extras
import os
import logging
from typing import Dict, Any, List, Tuple, Optional

log = logging.getLogger(__name__)

DB_REGULATIONS_TABLE_NAME = os.environ.get("DB_REGULATIONS_TABLE_NAME", "regulations")
DB_COMPONENTS_TABLE_NAME = os.environ.get("DB_COMPONENTS_TABLE_NAME", "regulations_component")
DEFAULT_COMPONENT_ID = int(os.environ.get("DEFAULT_COMPONENT_ID", 7))
DB_SCHEMA_FILE_PATH = os.environ.get("DB_SCHEMA_FILE_PATH", "/opt/airflow/sql/create_tables.sql")


class DatabaseManager:
    """Maneja la conexión a la BD de Airflow y asegura el esquema."""

    def __init__(self):
        self.connection = None
        self.cursor = None
        # Conexión a la BD de Airflow (servicio 'postgres') [cite: 30]
        self.db_host = os.environ.get("POSTGRES_HOST", "postgres")
        self.db_name = os.environ.get("POSTGRES_DB", "airflow")
        self.db_user = os.environ.get("POSTGRES_USER", "airflow")
        self.db_pass = os.environ.get("POSTGRES_PASSWORD", "airflow")
        self.db_port = os.environ.get("POSTGRES_PORT", "5432")

    def connect(self) -> bool:
        """Conecta a la BD Postgres, reemplazando Secrets Manager[cite: 31]."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name, user=self.db_user,
                password=self.db_pass, host=self.db_host, port=self.db_port
            )
            self.cursor = self.connection.cursor()
            log.info(f"Conectado exitosamente a la base de datos en {self.db_host}")
            return True
        except psycopg2.OperationalError as e:
            log.error(f"Error de conexión a la base de datos: {e}")
            return False

    def close(self):
        if self.cursor: self.cursor.close()
        if self.connection: self.connection.close()
        log.info("Conexión a la base de datos cerrada.")

    def ensure_schema_exists(self) -> None:
        """Ejecuta el DDL (CREATE TABLE IF NOT EXISTS)."""
        if not self.connection or not self.cursor:
            raise Exception("Database not connected")
        try:
            log.info(f"Asegurando que el esquema exista desde: {DB_SCHEMA_FILE_PATH}")
            with open(DB_SCHEMA_FILE_PATH, 'r') as f:
                sql_script = f.read()
            self.cursor.execute(sql_script)
            self.connection.commit()
            log.info("Esquema asegurado/validado exitosamente.")
        except FileNotFoundError:
            log.error(f"Archivo DDL no encontrado en {DB_SCHEMA_FILE_PATH}.")
            raise
        except Exception as e:
            log.error(f"Error ejecutando el script DDL: {e}")
            self.connection.rollback()
            raise

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Ejecuta una consulta SELECT."""
        if not self.cursor: raise Exception("Database not connected")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df: pd.DataFrame, table_name: str) -> int:
        """Inserción masiva. Lógica original de lambda.py."""
        if not self.connection or not self.cursor:
            raise Exception("Database not connected")
        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]
            psycopg2.extras.execute_batch(self.cursor, insert_query, records_to_insert)
            inserted_count = len(records_to_insert)
            self.connection.commit()
            log.info(f"Bulk insert en '{table_name}' exitoso. Filas afectadas: {inserted_count}")
            return inserted_count
        except Exception as e:
            self.connection.rollback()
            log.error(f"Error en bulk_insert en {table_name}: {str(e)}", exc_info=True)
            raise



def insert_regulations_component(db_manager: DatabaseManager, new_ids: List[int]) -> Tuple[int, str]:
    """Inserto componente. Lógica original de lambda.py."""
    if not new_ids:
        return 0, "No new regulation IDs provided"
    try:
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['components_id'] = DEFAULT_COMPONENT_ID
        inserted_count = db_manager.bulk_insert(id_rows, DB_COMPONENTS_TABLE_NAME)
        msg = f"Successfully inserted {inserted_count} regulation components"
        log.info(msg)
        return inserted_count, msg
    except Exception as e:
        msg = f"Error inserting regulation components: {str(e)}"
        log.error(msg, exc_info=True)
        return 0, msg


def insert_new_records(db_manager: DatabaseManager, df: pd.DataFrame, entity: str) -> Tuple[int, str]:
    """Inserta nuevos registros evitando duplicados. Lógica original de lambda.py."""
    regulations_table_name = DB_REGULATIONS_TABLE_NAME
    try:
        query = f"SELECT title, created_at, entity, COALESCE(external_link, '') as external_link FROM {regulations_table_name} WHERE entity = %s"
        existing_records = db_manager.execute_query(query, (entity,))
        db_df = pd.DataFrame(existing_records, columns=['title', 'created_at', 'entity',
                                                        'external_link']) if existing_records else pd.DataFrame(
            columns=['title', 'created_at', 'entity', 'external_link'])
        log.info(f"Registros existentes en BD para {entity}: {len(db_df)}")

        entity_df = df[df['entity'] == entity].copy()
        if entity_df.empty: return 0, f"No records found for entity {entity}"
        log.info(f"Registros a procesar para {entity}: {len(entity_df)}")

        if not db_df.empty:
            db_df['created_at'] = db_df['created_at'].astype(str)
            db_df['external_link'] = db_df['external_link'].fillna('').astype(str)
            db_df['title'] = db_df['title'].astype(str).str.strip()
        entity_df['created_at'] = entity_df['created_at'].astype(str)
        entity_df['external_link'] = entity_df['external_link'].fillna('').astype(str)
        entity_df['title'] = entity_df['title'].astype(str).str.strip()

        log.info("=== INICIANDO VALIDACIÓN DE DUPLICADOS OPTIMIZADA ===")
        if db_df.empty:
            new_records = entity_df.copy()
            duplicates_found = 0
            log.info("No hay registros existentes, todos son nuevos")
        else:
            entity_df['unique_key'] = entity_df['title'] + '|' + entity_df['created_at'] + '|' + entity_df[
                'external_link']
            db_df['unique_key'] = db_df['title'] + '|' + db_df['created_at'] + '|' + db_df['external_link']
            existing_keys = set(db_df['unique_key'])
            entity_df['is_duplicate'] = entity_df['unique_key'].isin(existing_keys)
            new_records = entity_df[~entity_df['is_duplicate']].copy()
            duplicates_found = len(entity_df) - len(new_records)
            if duplicates_found > 0: log.info(f"Duplicados encontrados: {duplicates_found}")

        len_before_internal_dedup = len(new_records)
        new_records = new_records.drop_duplicates(subset=['title', 'created_at', 'external_link'], keep='first')
        internal_duplicates = len_before_internal_dedup - len(new_records)
        if internal_duplicates > 0: log.info(f"Duplicados internos removidos: {internal_duplicates}")
        total_duplicates = duplicates_found + internal_duplicates
        log.info(f"=== DUPLICADOS IDENTIFICADOS: {total_duplicates} ===")
        if new_records.empty:
            return 0, f"No new records found for entity {entity} after duplicate validation"

        db_columns = ['created_at', 'update_at', 'is_active', 'title', 'gtype', 'entity', 'external_link', 'rtype_id',
                      'summary', 'classification_id']
        df_to_insert = new_records[[col for col in db_columns if col in new_records.columns]]
        log.info(f"Registros finales a insertar: {len(df_to_insert)}")

        total_rows_processed = 0
        try:
            total_rows_processed = db_manager.bulk_insert(df_to_insert, regulations_table_name)
            if total_rows_processed == 0: return 0, f"No records were actually inserted for entity {entity}"
            log.info(f"Registros insertados exitosamente: {total_rows_processed}")
        except Exception as insert_error:
            log.error(f"Error en inserción: {insert_error}", exc_info=True)
            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                log.warning("Error de duplicados detectado - algunos registros ya existían")
                return 0, f"Some records for entity {entity} were duplicates and skipped"
            else:
                raise insert_error

        log.info("=== OBTENIENDO IDS DE REGISTROS INSERTADOS ===")
        new_ids_query = f"SELECT id FROM {regulations_table_name} WHERE entity = %s ORDER BY id DESC LIMIT %s"
        new_ids_result = db_manager.execute_query(new_ids_query, (entity, total_rows_processed))
        new_ids = [row[0] for row in new_ids_result]
        log.info(f"IDs obtenidos: {len(new_ids)}")

        component_message = ""
        if new_ids:
            try:
                _, component_message = insert_regulations_component(db_manager, new_ids)
            except Exception as comp_error:
                log.error(f"Error insertando componentes: {comp_error}", exc_info=True)
                component_message = f"Error inserting components: {str(comp_error)}"

        stats = f"Processed: {len(entity_df)} | Existing: {len(db_df)} | Duplicates skipped: {total_duplicates} | New inserted: {total_rows_processed}"
        message = f"Entity {entity}: {stats}. {component_message}"
        log.info(f"=== RESULTADO FINAL ===\n{message}")

        return total_rows_processed, message
    except Exception as e:
        if hasattr(db_manager, 'connection') and db_manager.connection: db_manager.connection.rollback()
        error_msg = f"Error processing entity {entity}: {str(e)}"
        log.error(f"ERROR CRÍTICO: {error_msg}", exc_info=True)
        return 0, error_msg



def write_data(df: pd.DataFrame, entity: str) -> Dict[str, Any]:
    """Función principal de escritura que el DAG llamará."""
    if df.empty:
        log.warning("DataFrame vacío, no hay nada que escribir.")
        return {'records_inserted': 0, 'message': 'No data to write.'}
    db_manager = DatabaseManager()
    if not db_manager.connect():
        raise Exception("Fallo en la conexión de la base de datos para escritura.")
    try:
        inserted_count, status_message = insert_new_records(db_manager, df, entity)
        return {'records_inserted': inserted_count, 'message': status_message}
    except Exception as e:
        log.error(f"Error durante la escritura: {e}", exc_info=True)
        raise
    finally:
        if db_manager: db_manager.close()