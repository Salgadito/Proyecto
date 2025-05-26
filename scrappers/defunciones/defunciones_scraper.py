import asyncio
from typing import List, Optional
import pandas as pd
from aiohttp import ClientSession, TCPConnector
import ssl
import logging

# Configura el logging para ver advertencias y errores
logging.basicConfig(level=logging.INFO)


class DefuncionesScraper:
    def __init__(self, url: str, max_concurrent: int, verify_ssl: bool = False) -> None:
        self.url = url
        self.max_concurrent = max_concurrent
        self.verify_ssl = verify_ssl
        self.semaphore = asyncio.Semaphore(max_concurrent)

    def _build_session(self) -> ClientSession:
        if self.verify_ssl:
            return ClientSession()
        else:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            connector = TCPConnector(ssl=ssl_ctx)
            return ClientSession(connector=connector)

    async def _fetch(self, session: ClientSession, nuip: str) -> dict:
        payload = {"nuip": nuip}
        try:
            async with session.post(self.url, json=payload, timeout=10) as resp:
                data = await resp.json()
                vigencia = data.get("vigencia", "No disponible")
        except Exception as e:
            logging.warning(f"Error al consultar {nuip}: {e}")
            vigencia = "Error"
        return {"Documento": nuip, "Vigencia": vigencia}

    async def _limited_task(self, session: ClientSession, doc: str) -> dict:
        async with self.semaphore:
            return await self._fetch(session, doc)

    async def run(
        self, nuips: List[str],
        progress_bar: Optional[any] = None,
        progress_label: Optional[any] = None
    ) -> pd.DataFrame:
        async with self._build_session() as session:
            tasks = [self._limited_task(session, nuip) for nuip in nuips]
            total = len(tasks)
            resultados = []

            for count, coro in enumerate(asyncio.as_completed(tasks), start=1):
                res = await coro
                resultados.append(res)

                # Actualiza barra de progreso si se proporcionó
                if progress_bar:
                    frac = count / total
                    progress_bar.progress(frac)
                    if progress_label:
                        progress_label.text(f"{count} de {total} ({frac:.1%})")

        return pd.DataFrame(resultados)
