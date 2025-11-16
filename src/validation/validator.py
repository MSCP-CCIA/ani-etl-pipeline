import pandas as pd
import yaml
import logging
import re
from pydantic import BaseModel, create_model, field_validator, ValidationError, ConfigDict
from typing import Dict, Any, List, Optional, Type

log = logging.getLogger(__name__)


def load_rules(config_path: str) -> Dict[str, Any]:
    """Carga las reglas de validación desde un archivo YAML[cite: 26]."""
    try:
        with open(config_path, 'r') as f:
            rules = yaml.safe_load(f)
            log.info(f"Reglas de validación cargadas desde {config_path}")
            return rules
    except FileNotFoundError:
        log.error(f"Archivo de reglas {config_path} no encontrado.", exc_info=True)
        raise
    except yaml.YAMLError:
        log.error(f"Error parseando archivo YAML {config_path}.", exc_info=True)
        raise


def create_dynamic_validator(rules: Dict[str, Any]) -> Type[BaseModel]:
    """Crea un modelo Pydantic dinámicamente basado en las reglas del YAML."""

    class ArbitraryConfig:
        arbitrary_types_allowed = True

    field_definitions: Dict[str, Any] = {}
    validators: Dict[str, Any] = {}
    type_map = {'str': (Optional[str], None), 'int': (Optional[int], None), 'bool': (Optional[bool], None),
                'date': (Optional[pd.Timestamp], None)}

    # 1. Definir tipos de campos 
    for field, field_type in rules.get('types', {}).items():
        field_definitions[field] = type_map.get(field_type, (Optional[Any], None))

    # 2. Definir validadores de Regex
    for field, pattern in rules.get('regex', {}).items():
        def create_regex_validator(pattern=pattern):
            @field_validator(field, check_fields=False)
            @classmethod
            def validate_regex(cls, v: Optional[str]) -> Optional[str]:
                if v is None: return v
                if not re.match(pattern, str(v)):
                    raise ValueError(f"No cumple el patrón regex: {pattern}")
                return v

            return validate_regex

        validators[f"validate_regex_{field}"] = create_regex_validator()

    return create_model('DynamicRegulationModel',__config__=ConfigDict(arbitrary_types_allowed=True),**field_definitions,__validators__=validators)


def validate_data(df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
    """Función principal de validación. Implementa la lógica de descarte/anulación."""
    if df.empty:
        log.warning("DataFrame vacío, omitiendo validación.")
        return df

    original_count = len(df)
    log.info(f"Iniciando validación de {original_count} registros...")
    DynamicModel = create_dynamic_validator(rules)
    required_fields = rules.get('required', [])
    validated_rows: List[Dict[str, Any]] = []
    discarded_row_count = 0

    if 'created_at' in df.columns: df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    if 'rtype_id' in df.columns: df['rtype_id'] = pd.to_numeric(df['rtype_id'], errors='coerce')

    records = df.to_dict('records')

    for i, row in enumerate(records):
        missing_required = [field for field in required_fields if pd.isna(row.get(field))]
        if missing_required:
            log.warning(
                f"Fila {i} (Titulo: {row.get('title')}) descartada. Faltan campos obligatorios: {missing_required}")
            discarded_row_count += 1
            continue

        try:
            validated_model = DynamicModel.model_validate(row)
            validated_rows.append(validated_model.model_dump())
        except ValidationError as e:
            log.warning(f"Errores de validación en Fila {i} (Titulo: {row.get('title')}). Anulando campos.")
            validated_row_data = row.copy()
            for error in e.errors():
                try:
                    field_name = error['loc'][0]
                    if field_name not in required_fields:
                        log.debug(f"Anulando campo no obligatorio: {field_name} (Error: {error['msg']})")
                        validated_row_data[field_name] = None
                except IndexError:
                    log.warning(f"Error de validación genérico: {error['msg']}")

            try:
                final_validated_model = DynamicModel.model_validate(validated_row_data)
                validated_rows.append(final_validated_model.model_dump())
            except ValidationError as final_e:
                log.error(f"Fila {i} descartada permanentemente tras intento de anulación. Error: {final_e}")
                discarded_row_count += 1

    final_df = pd.DataFrame(validated_rows)
    final_count = len(final_df)
    log.info(
        f"Validación completa. Originales: {original_count}, Descartados: {discarded_row_count}, Válidos: {final_count}")
    return final_df
