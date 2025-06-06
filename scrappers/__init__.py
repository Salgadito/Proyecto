from .defunciones.defunciones_scraper import DefuncionesScraper
from .deudores.deudores_scraper import DeudoresScraper
from .Pep.pep_scrapper import FuncionPublicaScraper
from .Ofac.ofac_scraper import  UniversalModularSDNChecker
from .EU.eu_scrapper import UniversalModularEUChecker

SCRAPER_CLASSES = {
    "Defunciones Registraduría": DefuncionesScraper,
    "Morosidad Judicial": DeudoresScraper,
    "Declaraciones Función Pública": FuncionPublicaScraper,
    "Lista OFAC (SDN)": UniversalModularSDNChecker,
    "Unión Europea": UniversalModularEUChecker
}



