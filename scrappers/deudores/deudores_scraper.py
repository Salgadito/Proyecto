import asyncio
from typing import List
import pandas as pd
from aiohttp import ClientSession, TCPConnector
import logging
import random

# Configura el logging
logging.basicConfig(level=logging.INFO)

class DeudoresScraper:
    """
    Scraper asíncrono para consultar morosidad en Rama Judicial.
    Usa semáforo y connector para limitar concurrencia.
    """

    def __init__(self, url: str, max_concurrent: int, max_retries: int = 3) -> None:
        """
        Parameters
        ----------
        url : str
            Endpoint para POST.
        max_concurrent : int
            Máximo de peticiones concurrentes.
        max_retries : int
            Máximo de reintentos por documento.
        """
        self.url = url
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _fetch(self, session: ClientSession, doc: str) -> dict:
        """
        Hace POST y procesa respuesta de morosidad.

        Returns
        -------
        dict
            {'Documento': doc, 'Sancionado': ..., 'Estado': ...}.
        """
        payload = {"Documento": doc}
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.post(self.url, json=payload, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        total = data.get("Total", 0)
                        items = data.get("Data", [])
                        if total and items:
                            sancionado = items[0].get("Sancionado")
                            estado = "Moroso"
                        else:
                            sancionado = None
                            estado = "No moroso"
                    else:
                        sancionado = None
                        estado = f"Error {resp.status}"
                    return {"Documento": doc, "Sancionado": sancionado, "Estado": estado}
            except Exception as e:
                logging.warning(f"Intento {attempt} fallido para {doc}: {e}")
                if attempt < self.max_retries:
                    wait = random.uniform(1, 3) * attempt
                    await asyncio.sleep(wait)
                else:
                    return {"Documento": doc, "Sancionado": None, "Estado": "Error"}

    async def _limited_task(self, session: ClientSession, doc: str) -> dict:
        """
        Wrapper que aplica el semáforo para limitar concurrencia.
        """
        async with self.semaphore:
            return await self._fetch(session, doc)

    async def run(
        self,
        nuips: List[str],
        progress_bar,
        progress_label,
    ) -> pd.DataFrame:
        """
        Ejecuta las consultas de morosidad y actualiza UI.

        Parameters
        ----------
        nuips : List[str]
        progress_bar : st.Progress
        progress_label : st.Empty

        Returns
        -------
        pd.DataFrame
        """
        connector = TCPConnector(limit_per_host=self.max_concurrent)

        async with ClientSession(connector=connector) as session:
            tasks = [self._limited_task(session, str(doc)) for doc in nuips]
            total = len(tasks)
            resultados = []
            for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
                resultados.append(await coro)
                frac = idx / total
                progress_bar.progress(frac)
                progress_label.text(f"{idx} de {total} ({frac:.1%})")

        return pd.DataFrame(resultados)
