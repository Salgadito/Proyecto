"""
Configuraciones para cada scrapper.
Cada clave es un identificador de scrapper, y su valor un dict con parámetros.
"""

SCRAPERS = {
    "Defunciones Registraduría": {
        "url": "https://defunciones.registraduria.gov.co:8443/VigenciaCedula/consulta",
        "max_concurrent": 100,
        "ip_interval": 1000,
    },
    "Morosidad Judicial": {
        "url": "https://cobrocoactivo.ramajudicial.gov.co/Home/Bdme_Read",
        "max_concurrent": 100,
    },
}
