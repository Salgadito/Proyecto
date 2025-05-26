"""
Registro de clases de scrappers.
Al añadir un nuevo scrapper, importarlo aquí y añadirlo a SCRAPER_CLASSES.
"""

from .defunciones.defunciones_scraper import DefuncionesScraper
from .deudores.deudores_scraper import DeudoresScraper

SCRAPER_CLASSES = {
    "Defunciones Registraduría": DefuncionesScraper,
    "Morosidad Judicial": DeudoresScraper,
}
