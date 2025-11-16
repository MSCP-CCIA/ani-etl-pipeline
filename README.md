# Pipeline ETL de Normatividad ANI

## Resumen

Este proyecto implementa un pipeline ETL orquestado con Apache Airflow para la extracción, validación y carga de las normativas de la Agencia Nacional de Infraestructura (ANI).

El sistema refactoriza un script monolítico de AWS Lambda a un pipeline de datos modular, idempotente y configurable, ejecutado íntegramente sobre Docker.

## Despliegue y Ejecución

Todo el entorno se gestiona con `make` y `docker-compose`.

### 1. Prerrequisitos

* Docker
* `docker-compose` (V1) o `docker-compose-plugin` (V2)
* `make`

### 2. Configuración

El proyecto es configurable a través de variables de entorno (**No se encuentran en el Repositorio**).

1.  Asegúrese de que el archivo `.env` esté presente en la raíz del proyecto.
2.  (Opcional) Ajuste las variables en `.env` según sea necesario. Los valores por defecto están listos para la ejecución.

### 3. Levantar el Entorno

Este comando limpia ejecuciones anteriores, inicializa la base de datos de Airflow, crea el usuario `admin` y levanta todos los servicios en modo *detached*.

```bash
make start
```

### 4. Ejecutar el Pipeline

1.  **Acceda a la Interfaz Web:**
    * **URL:** `http://localhost:8080` (o el DNS de su servidor).
    * **Usuario:** `admin`
    * **Contraseña:** `admin`

2.  **Ejecute el DAG:**
    * En la página principal, active el DAG `ani_etl_pipeline` (moviendo el interruptor).
    * Presione el botón "Play" a la derecha para iniciar una ejecución manual.

## Arquitectura y Diseño

El pipeline está diseñado para una clara separación de responsabilidades y sigue el flujo solicitado.

**Flujo de Tareas del DAG:**
`ensure_schema` → `extract_data` → `validate_data` → `write_data`

* **`ensure_schema`**: Tarea de preparación que ejecuta el DDL para crear las tablas si no existen.
* **`extract_data`**: Ejecuta el scraping (lógica original preservada) y pasa los datos por referencia (guardando un archivo Parquet en `/tmp/`).
* **`validate_data`**: Carga el archivo, aplica las reglas desde `configs/validation_rules.yml` y guarda un nuevo archivo Parquet.
* **`write_data`**: Carga los datos validados y los inserta en la base de datos de Airflow, utilizando la lógica de idempotencia original preservada.

---
## Estructura del Repositorio

```
/ani-etl-pipeline/
├── .env              # Configuración de entorno (local, no en git)
├── .gitignore        # Ignora logs, .env, y archivos de caché
├── configs/
│   └── validation_rules.yml  # Reglas de validación configurables
├── dags/
│   ├── __init__.py
│   └── ani_etl_dag.py        # Definición del DAG (4 Tareas)
├── sql/
│   └── create_tables.sql     # Script DDL idempotente
├── src/
│   ├── __init__.py
│   ├── extraction/
│   │   ├── __init__.py
│   │   └── scraper.py      # Módulo de Extracción (Lógica de scraping original)
│   ├── persistence/
│   │   ├── __init__.py
│   │   └── writer.py       # Módulo de Escritura (Lógica de idempotencia original)
│   └── validation/
│       ├── __init__.py
│       └── validator.py    # Módulo de Validación (Nuevo)
├── Dockerfile              # (Provisto)
├── Makefile                # (Modificado para compatibilidad)
├── docker-compose.yml      # (Modificado para montar 'sql/' y 'configs/')
├── requirements.txt        # (Actualizado con Pydantic, PyYAML, PyArrow)
└── README.md               # Este archivo
```