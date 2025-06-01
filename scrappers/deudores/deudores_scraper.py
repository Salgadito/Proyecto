import asyncio
from typing import List, Dict, Any  # Any importado para type hints
import pandas as pd
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import logging
import random

# Configura el logging con un formato más detallado
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DeudoresScraper:
    """
    Scraper asíncrono para consultar morosidad en un endpoint (ej. Rama Judicial).
    Usa semáforo y conector TCP para limitar concurrencia y reintentos con backoff.
    """

    def __init__(
        self,
        url: str,
        max_concurrent: int,
        max_retries: int = 3,
        timeout_seconds: int = 30,
    ) -> None:
        """
        Parameters
        ----------
        url : str
            Endpoint para realizar la petición POST.
        max_concurrent : int
            Número máximo de peticiones concurrentes.
        max_retries : int, opcional
            Número máximo de reintentos por documento en caso de fallo. Por defecto es 3.
        timeout_seconds : int, opcional
            Timeout total en segundos para cada petición. Por defecto es 30.
        """
        self.url = url
        if max_concurrent <= 0:
            raise ValueError("max_concurrent debe ser un entero positivo.")
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.timeout = ClientTimeout(total=timeout_seconds)
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _fetch(self, session: ClientSession, doc: str) -> Dict[str, Any]:
        """
        Realiza la petición POST a la URL para un documento y procesa la respuesta.
        Incluye lógica de reintentos con backoff exponencial jittered.

        Parameters
        ----------
        session : ClientSession
            Sesión de aiohttp para realizar la petición.
        doc : str
            Número de documento a consultar.

        Returns
        -------
        dict
            Un diccionario con 'Documento', 'Sancionado' (nombre del sancionado o None),
            y 'Estado' ('Moroso', 'No moroso', o un mensaje de error).
        """
        payload = {"Documento": doc}  # Payload definido una vez

        for attempt in range(1, self.max_retries + 1):
            try:
                logging.debug(
                    f"Documento {doc}: Iniciando intento {attempt}/{self.max_retries}."
                )
                async with session.post(
                    self.url, json=payload, timeout=self.timeout
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        items = data.get("Data", [])
                        if isinstance(items, list) and items:  # Verificar si es lista y no vacía
                            sancionado_info = items[0]
                            sancionado_nombre = sancionado_info.get("Sancionado")
                            estado = "Moroso"
                            logging.info(
                                f"Documento {doc}: Encontrado como Moroso. Sancionado: {sancionado_nombre}"
                            )
                        else:
                            sancionado_nombre = None
                            estado = "No moroso"
                            logging.info(f"Documento {doc}: No encontrado como moroso.")
                        return {
                            "Documento": doc,
                            "Sancionado": sancionado_nombre,
                            "Estado": estado,
                        }
                    elif (
                        400 <= resp.status < 500 and resp.status not in {408, 429}
                    ):  # Errores cliente no reintentables
                        logging.warning(
                            f"Documento {doc}: Error Cliente {resp.status} (intento {attempt}). No se reintentará."
                        )
                        error_message = await resp.text()
                        logging.debug(
                            f"Documento {doc}: Respuesta del error: {error_message[:200]}"
                        )  # Log primeros 200 chars
                        return {
                            "Documento": doc,
                            "Sancionado": None,
                            "Estado": f"Error Cliente {resp.status}",
                        }
                    else:  # Otros errores (5xx, 408, 429) que podrían reintentarse
                        logging.warning(
                            f"Documento {doc}: Error HTTP {resp.status} (intento {attempt})."
                        )
                        # Continúa al bloque de reintento si quedan intentos

            except asyncio.TimeoutError:
                logging.warning(
                    f"Documento {doc}: Timeout en intento {attempt}/{self.max_retries}."
                )
            except Exception as e:  # Cubre ClientConnectionError, ClientError, etc.
                logging.warning(
                    f"Documento {doc}: Excepción en intento {attempt}/{self.max_retries} - {type(e).__name__}: {e}"
                )

            # Lógica de reintento
            if attempt < self.max_retries:
                base_delay = 0.5  # Segundos
                wait_time = (2 ** (attempt - 1)) * base_delay * random.uniform(0.8, 1.2)
                logging.info(
                    f"Documento {doc}: Reintentando en {wait_time:.2f} segundos..."
                )
                await asyncio.sleep(wait_time)
            else:
                logging.error(
                    f"Documento {doc}: Fallaron todos los {self.max_retries} intentos."
                )
                return {
                    "Documento": doc,
                    "Sancionado": None,
                    "Estado": "Error (Máximos reintentos alcanzados)",
                }

        # Fallback, aunque no debería alcanzarse si la lógica es correcta
        return {
            "Documento": doc,
            "Sancionado": None,
            "Estado": "Error Inesperado en _fetch",
        }

    async def _limited_task(
        self, session: ClientSession, doc: str
    ) -> Dict[str, Any]:
        """
        Wrapper para la tarea _fetch que aplica el semáforo para limitar concurrencia.
        """
        async with self.semaphore:
            return await self._fetch(session, doc)

    async def run(
        self,
        nuips: List[str],
        progress_bar,  # Espera un objeto de barra de progreso (ej. Streamlit)
        progress_label,  # Espera un objeto de etiqueta (ej. Streamlit)
    ) -> pd.DataFrame:
        """
        Ejecuta las consultas de morosidad de forma asíncrona para una lista de NUIPs.
        Actualiza la barra de progreso y etiqueta de texto proporcionadas.

        Parameters
        ----------
        nuips : List[str]
            Lista de números de documento (NUIPs) a consultar.
        progress_bar : object
            Objeto de barra de progreso compatible con el método `progress(float)`.
        progress_label : object
            Objeto de etiqueta de texto compatible con el método `text(str)`.

        Returns
        -------
        pd.DataFrame
            Un DataFrame con los resultados de la consulta, con columnas
            ['Documento', 'Sancionado', 'Estado'].
        """
        if not nuips:
            logging.info("La lista de NUIPs a procesar está vacía.")
            return pd.DataFrame(columns=["Documento", "Sancionado", "Estado"])

        connector = TCPConnector(
            limit_per_host=self.max_concurrent, force_close=False
        )  # force_close=False para keep-alive

        resultados = []
        # Asegurarse de que doc sea string y no esté vacío/solo espacios
        valid_nuips = [str(doc).strip() for doc in nuips if str(doc).strip()]

        if not valid_nuips:
            logging.info("La lista de NUIPs procesables está vacía después de filtrar.")
            # Devolver resultados para las entradas originales como no procesadas o vacías si es necesario
            # o simplemente un DataFrame vacío si no hay nada que procesar.
            # Por ahora, devolvemos un DF vacío.
            return pd.DataFrame(columns=["Documento", "Sancionado", "Estado"])


        async with ClientSession(connector=connector) as session:
            tasks = [self._limited_task(session, doc) for doc in valid_nuips]
            
            total_tasks = len(tasks)
            logging.info(
                f"Iniciando procesamiento de {total_tasks} documentos con max_concurrent={self.max_concurrent}."
            )

            completed_count = 0
            for future in asyncio.as_completed(tasks):
                try:
                    result = await future
                    resultados.append(result)
                except Exception as e:
                    # Esta excepción se capturaría si la tarea en sí (_limited_task o _fetch)
                    # fallara de una manera no controlada internamente (lo cual no debería ocurrir).
                    logging.error(
                        f"Error inesperado al procesar una tarea completada: {e}"
                    )
                    # Podríamos añadir un resultado genérico de error si el documento asociado se pudiera determinar.
                    # Dado que _fetch está diseñado para devolver siempre un dict, este path es menos probable.
                
                completed_count += 1
                frac = completed_count / total_tasks
                if progress_bar: # Verificar si el objeto existe
                    progress_bar.progress(frac)
                if progress_label: # Verificar si el objeto existe
                    progress_label.text(
                        f"Procesados {completed_count} de {total_tasks} ({frac:.1%})"
                    )

        logging.info(
            f"Procesamiento completado. {len(resultados)} resultados obtenidos."
        )
        return pd.DataFrame(resultados)