import requests
import logging
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import os
import json

log = logging.getLogger(__name__)

try:
    URL_BASE = os.environ["ANI_SCRAPER_URL_BASE"]
    ENTITY_VALUE = os.environ["ANI_ENTITY_VALUE"]
    keywords_json_string = os.environ.get("ANI_RTYPE_KEYWORDS", "{}")
    CLASSIFICATION_KEYWORDS = json.loads(keywords_json_string)
    log.info(f"Palabras clave de clasificación cargadas: {CLASSIFICATION_KEYWORDS}")
except KeyError as e:
    log.error(f"Variable de entorno crítica no definida: {e}")
    raise
except json.JSONDecodeError as e:
    log.error(f"Error parseando ANI_RTYPE_KEYWORDS: {e}")
    CLASSIFICATION_KEYWORDS = {}

FIXED_CLASSIFICATION_ID = int(os.environ.get("ANI_FIXED_CLASSIFICATION_ID", 13))
DEFAULT_RTYPE_ID = int(os.environ.get("ANI_DEFAULT_RTYPE_ID", 14))

def clean_quotes(text: Optional[str]) -> Optional[str]:
    """Elimina varios tipos de comillas y espacios extra de un texto."""
    if not text:
        return text
    quotes_map = {
        '\u201C': '', '\u2018': '', '\u2019': '', '\u00AB': '', '\u00BB': '',
        '\u201E': '', '\u201A': '', '\u2039': '', '\u203A': '', '"': '',
        "'": '', '´': '', '`': '', '′': '', '″': '',
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)
    quotes_pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]'
    cleaned_text = re.sub(quotes_pattern, '', cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text


def get_rtype_id(title: str) -> int:
    """Obtiene el rtype_id basado en el título del documento."""
    title_lower = title.lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID


def is_valid_created_at(created_at_value: Any) -> bool:
    """Valida el campo created_at."""
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False


def extract_title_and_link(row: Any, norma_data: Dict[str, Any], verbose: bool, row_num: int) -> bool:
    """Extrae título y enlace. Contiene la lógica de omisión original. """
    title_cell = row.find('td', class_='views-field views-field-title')
    if not title_cell:
        if verbose: log.warning(f"No se encontró celda de título en la fila {row_num}. Saltando.")
        return False
    title_link = title_cell.find('a')
    if not title_link:
        if verbose: log.warning(f"No se encontró enlace en la fila {row_num}. Saltando.")
        return False
    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)
    if len(cleaned_title) > 65:  # Lógica de negocio original
        if verbose: log.warning(f"Saltando norma con título demasiado largo: '{cleaned_title}'")
        return False
    norma_data['title'] = cleaned_title
    external_link = title_link.get('href')
    if external_link and not external_link.startswith('http'):
        external_link = 'https://www.ani.gov.co' + external_link
    norma_data['external_link'] = external_link
    norma_data['gtype'] = 'link' if external_link else None
    if not norma_data['external_link']:
        if verbose: log.warning(f"Saltando norma '{norma_data['title']}' por no tener enlace.")
        return False
    return True


def extract_summary(row: Any, norma_data: Dict[str, Any]):
    """Extrae el resumen/descripción de una fila."""
    summary_cell = row.find('td', class_='views-field views-field-body')
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        formatted_summary = cleaned_summary.capitalize() if cleaned_summary else None
        norma_data['summary'] = formatted_summary
    else:
        norma_data['summary'] = None


def extract_creation_date(row: Any, norma_data: Dict[str, Any], verbose: bool, row_num: int) -> bool:
    """Extrae la fecha de creación. Contiene la lógica de validación/omisión original. """
    fecha_cell = row.find('td', class_='views-field views-field-field-fecha--1')
    if fecha_cell:
        fecha_span = fecha_cell.find('span', class_='date-display-single')
        if fecha_span:
            created_at_raw = fecha_span.get('content', fecha_span.get_text(strip=True))
            if 'T' in created_at_raw:
                norma_data['created_at'] = created_at_raw.split('T')[0]
            elif '/' in created_at_raw:
                try:
                    day, month, year = created_at_raw.split('/')
                    norma_data['created_at'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    norma_data['created_at'] = created_at_raw
            else:
                norma_data['created_at'] = created_at_raw
        else:
            norma_data['created_at'] = fecha_cell.get_text(strip=True)
    else:
        norma_data['created_at'] = None
    if not is_valid_created_at(norma_data['created_at']):
        if verbose: log.warning(f"Saltando norma '{norma_data.get('title')}' por fecha inválida.")
        return False
    return True


def scrape_page(page_num: int, verbose: bool = False) -> List[Dict[str, Any]]:
    """Scrapea una página específica de ANI."""
    if page_num == 0:
        page_url = URL_BASE
    else:
        page_url = f"{URL_BASE}&page={page_num}"
    if verbose: log.info(f"Scrapeando página {page_num}: {page_url}")
    try:
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        tbody = soup.find('tbody')
        if not tbody:
            if verbose: log.info(f"No se encontró tabla en página {page_num}")
            return []
        rows = tbody.find_all('tr')
        if verbose: log.info(f"Encontradas {len(rows)} filas en página {page_num}")
        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                norma_data = {
                    'created_at': None, 'update_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'is_active': True, 'title': None, 'gtype': None, 'entity': ENTITY_VALUE,
                    'external_link': None, 'rtype_id': None, 'summary': None,
                    'classification_id': FIXED_CLASSIFICATION_ID,
                }
                if not extract_title_and_link(row, norma_data, verbose, i): continue
                extract_summary(row, norma_data)
                if not extract_creation_date(row, norma_data, verbose, i): continue
                norma_data['rtype_id'] = get_rtype_id(norma_data['title'])
                page_data.append(norma_data)
            except Exception as e:
                if verbose: log.error(f"Error procesando fila {i}: {str(e)}")
                continue
        return page_data
    except requests.RequestException as e:
        log.error(f"Error HTTP en página {page_num}: {e}")
        return []
    except Exception as e:
        log.error(f"Error procesando página {page_num}: {e}")
        return []

def extract_data(num_pages_to_scrape: int) -> pd.DataFrame:
    """Función principal de extracción que el DAG llamará."""
    log.info(f"Iniciando extracción de {num_pages_to_scrape} páginas...")
    all_normas_data = []
    for page_num in range(num_pages_to_scrape):
        log.info(f"Procesando página {page_num}...")
        try:
            page_data = scrape_page(page_num, verbose=True)
            all_normas_data.extend(page_data)
        except Exception as e:
            log.error(f"Error procesando página {page_num}: {e}", exc_info=True)

    if not all_normas_data:
        log.warning("No se encontraron datos en el scraping.")
        return pd.DataFrame()

    df_normas = pd.DataFrame(all_normas_data)
    log.info(f"Extracción completa. Total registros extraídos: {len(df_normas)}")
    return df_normas