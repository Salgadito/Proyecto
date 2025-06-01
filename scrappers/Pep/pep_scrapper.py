import asyncio
import aiohttp
import pandas as pd
import re
from bs4 import BeautifulSoup
from aiohttp import ClientSession, TCPConnector
from asyncio import Semaphore

class FuncionPublicaScraper:
    def __init__(self, max_concurrent=1000, max_retries=3):
        self.BASE_URL = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index"
        self.HEADERS = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        self.semaphore = Semaphore(max_concurrent)
        self.results = []
        self.max_retries = max_retries

    async def fetch_declaraciones(self, session: ClientSession, cedula: str):
        params = {
            "tipoPersonaId": "25",
            "primerNombre": "",
            "segundoNombre": "",
            "primerApellido": "",
            "segundoApellido": "",
            "numeroDocumento": cedula,
            "entidad": "",
            "fechaFinalizacionDesde": "",
            "fechaFinalizacionHasta": "",
            "find": "Buscar"
        }

        async with self.semaphore:
            attempt = 0
            while attempt < self.max_retries:
                try:
                    async with session.get(self.BASE_URL, params=params, headers=self.HEADERS, timeout=30) as response:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")
                        filas = soup.select("table.table tbody tr")
                        encontrados = False

                        for fila in filas:
                            cedula_raw = fila.select_one("td > p:nth-of-type(2)")
                            if cedula_raw:
                                match = re.search(r'CEDULA DE CIUDADANIA\s*-\s*(\d+)', cedula_raw.text)
                                if match and match.group(1).strip() == cedula:
                                    encontrados = True
                                    self.results.append({
                                        "Documento": cedula,
                                        "Declarante": fila.select_one("td > p:nth-of-type(1)").text.strip(),
                                        "Entidad": fila.select("td")[2].text.strip(),
                                        "Cargo": fila.select("td")[3].text.strip(),
                                        "Tipo Declaración": fila.select("td")[4].text.strip(),
                                        "Declaración N°": fila.select("td")[5].text.strip(),
                                        "Fecha Publicación": fila.select("td")[6].text.strip(),
                                        "Estado": fila.select("td")[7].text.strip()
                                    })

                        if not encontrados:
                            self.results.append({
                                "Documento": cedula,
                                "Declarante": "No existe",
                                "Entidad": "No existe",
                                "Cargo": "No existe",
                                "Tipo Declaración": "No existe",
                                "Declaración N°": "No existe",
                                "Fecha Publicación": "No existe",
                                "Estado": "No existe"
                            })
                        return  # ✅ Si tuvo éxito, salimos del loop

                except Exception:
                    attempt += 1
                    if attempt == self.max_retries:
                        self.results.append({
                            "Documento": cedula,
                            "Declarante": "Error",
                            "Entidad": "Error",
                            "Cargo": "Error",
                            "Tipo Declaración": "Error",
                            "Declaración N°": "Error",
                            "Fecha Publicación": "Error",
                            "Estado": "Error"
                        })
                    else:
                        await asyncio.sleep(2)  # ⏱️ Espera antes de reintentar (puedes ajustar este valor)



    async def run_async(self, nuips, progress_bar=None, progress_label=None):
        connector = TCPConnector(limit_per_host=10)
        async with ClientSession(connector=connector) as session:
            tasks = []
            for i, cedula in enumerate(nuips):
                tasks.append(self.fetch_declaraciones(session, cedula))
                if progress_bar and progress_label:
                    progress_bar.progress(i / len(nuips))
                    progress_label.text(f"{i + 1}/{len(nuips)} documentos")

            await asyncio.gather(*tasks)

    # ✅ Esta es la única parte que se cambia: la función ahora es async
    async def run(self, nuips, progress_bar=None, progress_label=None):
        await self.run_async(nuips, progress_bar, progress_label)
        return pd.DataFrame(self.results)
