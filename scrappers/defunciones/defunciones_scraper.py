import asyncio
from typing import List
import pandas as pd
from aiohttp import ClientSession


class DefuncionesScraper:
    """
    Scraper asíncrono para consultar vigencia de cédula en
    Registraduría. Controla concurrencia con un semáforo.
    """
    def __init__(self, url: str, max_concurrent: int) -> None:
        """
        Parameters
        ----------
        url : str
            Endpoint de la Registraduría para POST.
        max_concurrent : int
            Máximo de peticiones concurrentes.
        """
        self.url = url
        self.max_concurrent = max_concurrent

    async def _fetch(self, session: ClientSession, nuip: str) -> dict:
        """
        Realiza una petición POST para un número de documento.
        Devuelve un dict con el documento y su vigencia.
        """
        payload = {"nuip": nuip}
        try:
            async with session.post(self.url, json=payload, timeout=10) as resp:
                data = await resp.json()
                vigencia = data.get("vigencia", "No disponible")
        except Exception:
            vigencia = "Error"
        return {"Documento": nuip, "Vigencia": vigencia}

    async def run(
        self,
        nuips: List[str],
        progress_bar,
        progress_label
    ) -> pd.DataFrame:
        """
        Ejecuta las consultas de vigencia de forma asíncrona,
        actualiza la UI de Streamlit con barra y texto, y
        retorna un DataFrame con los resultados.

        Parameters
        ----------
        nuips : List[str]
            Lista de números de documento.
        progress_bar : st.Progress
            Barra de progreso de Streamlit.
        progress_label : st.Empty
            Texto de progreso de Streamlit.
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        tasks: List[asyncio.Task] = []
        total = len(nuips)

        async with ClientSession() as session:
            for nuip in nuips:
                async def limited_task(doc: str):
                    async with semaphore:
                        return await self._fetch(session, doc)

                tasks.append(limited_task(nuip))

            resultados: List[dict] = []
            for count, task in enumerate(asyncio.as_completed(tasks), start=1):
                res = await task
                resultados.append(res)

                frac = count / total
                progress_bar.progress(frac)
                progress_label.text(f"{count} de {total} ({frac:.1%})")

        return pd.DataFrame(resultados)
