# defunciones_scraper.py

import asyncio
from typing import List, Protocol
import pandas as pd
from aiohttp import ClientSession

from utils.ip_generator import generate_ip


class IPProvider(Protocol):
    """Protocolo para un proveedor de IPs rotativas."""
    def get_ip(self) -> str:
        ...


class DefaultIPProvider:
    """Proveedor de IP que llama a `generate_ip()` para obtener una nueva IP."""
    def get_ip(self) -> str:
        return generate_ip()


class DefuncionesScraper:
    """
    Scraper asíncrono para consultar vigencia de cédula en
    Registraduría. Controla concurrencia con un semáforo y
    rota IP cada `ip_interval` peticiones.
    """
    def __init__(
        self,
        url: str,
        max_concurrent: int,
        ip_interval: int,
        ip_provider: IPProvider | None = None
    ) -> None:
        """
        Parameters
        ----------
        url : str
            Endpoint de la Registraduría para POST.
        max_concurrent : int
            Máximo de peticiones concurrentes.
        ip_interval : int
            Renovación de IP cada `ip_interval` peticiones.
        ip_provider : IPProvider, optional
            Proveedor de IP; por defecto usa `DefaultIPProvider`.
        """
        self.url = url
        self.max_concurrent = max_concurrent
        self.ip_interval = ip_interval
        self.ip_provider = ip_provider or DefaultIPProvider()

    async def _fetch(self, session: ClientSession, nuip: str, ip: str) -> dict:
        """
        Realiza una petición POST para un número de documento.
        Devuelve un dict con el documento y su vigencia.
        """
        payload = {"nuip": nuip, "ip": ip}
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
            current_ip = self.ip_provider.get_ip()

            for idx, nuip in enumerate(nuips):
                # Rota IP cada ip_interval peticiones (pero no en la primera)
                if idx > 0 and idx % self.ip_interval == 0:
                    current_ip = self.ip_provider.get_ip()

                # Creamos una tarea limitada por el semáforo
                async def limited_task(doc: str, ip_addr: str):
                    async with semaphore:
                        return await self._fetch(session, doc, ip_addr)

                tasks.append(limited_task(nuip, current_ip))

            resultados: List[dict] = []
            # A medida que cada tarea completa, actualizamos la UI
            for count, task in enumerate(asyncio.as_completed(tasks), start=1):
                res = await task
                resultados.append(res)

                frac = count / total
                progress_bar.progress(frac)
                progress_label.text(f"{count} de {total} ({frac:.1%})")

        # Convertimos la lista de dicts a DataFrame
        return pd.DataFrame(resultados)