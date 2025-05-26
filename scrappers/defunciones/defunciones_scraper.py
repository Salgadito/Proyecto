import asyncio
from typing import List
import pandas as pd
from aiohttp import ClientSession


class DefuncionesScraper:
    def __init__(self, url: str, max_concurrent: int) -> None:
        self.url = url
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _fetch(self, session: ClientSession, nuip: str) -> dict:
        payload = {"nuip": nuip}
        try:
            async with session.post(self.url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        vigencia = data.get("vigencia", "No disponible")
                    except Exception:
                        vigencia = "Error de JSON"
                else:
                    vigencia = f"HTTP {resp.status}"
        except Exception:
            vigencia = "Error de red"
            return {"Documento": nuip, "Vigencia": vigencia}


    async def _limited_task(self, session: ClientSession, doc: str) -> dict:
        async with self.semaphore:
            return await self._fetch(session, doc)

    async def run(self, nuips: List[str], progress_bar, progress_label) -> pd.DataFrame:
        async with ClientSession() as session:
            tasks = [self._limited_task(session, nuip) for nuip in nuips]
            total = len(tasks)
            resultados = []

            for count, coro in enumerate(asyncio.as_completed(tasks), start=1):
                res = await coro
                resultados.append(res)
                frac = count / total
                progress_bar.progress(frac)
                progress_label.text(f"{count} de {total} ({frac:.1%})")

        return pd.DataFrame(resultados)
