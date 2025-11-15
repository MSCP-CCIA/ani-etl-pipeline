# Pipeline de ETL de Normatividad ANI

Este proyecto implementa un pipeline de ETL (Extracción, Validación, Carga) orquestado con Apache Airflow para procesar las normativas del sitio web de la ANI.

## Entregables

* **Código Refactorizado:** El código está modularizado en `src/` en etapas: `extraction` , `validation` , y `persistence`.
* **DAG de Airflow:** El archivo `dags/ani_etl_dag.py` define el pipeline. El flujo lógico de datos son las tres tareas (`extract_data`, `validate_data`, `write_data`), precedidas por una tarea de preparación (`ensure_schema`).
* **Archivo de Reglas:** El archivo `configs/validation_rules.yml` contiene las reglas de validación (tipos, regex, obligatoriedad) de forma configurable.
* **Esquema DDL:** El archivo `sql/create_tables.sql` contiene el DDL. La tarea `ensure_schema` del DAG ejecuta este archivo automáticamente para crear las tablas si no existen.
* **Logs Claros:** Los logs de cada tarea mostrarán los totales extraídos, descartes por validación y filas insertadas.

## Cómo Levantar el Entorno y Ejecutar

### Prerrequisitos

* Docker
* Docker Compose

### 1. Configuración del Entorno

El proyecto se configura enteramente a través del archivo `.env`. Asegúrese de que este archivo esté presente en la raíz del proyecto.

### 2. Levantar el Entorno de Airflow

Se utiliza el `Makefile` provisto para simplificar la gestión del entorno Docker.

```bash
# Este comando reinicia todo, inicializa Airflow, y levanta los servicios
make start