import streamlit as st
import pandas as pd
import asyncio
import random
from time import perf_counter
from aiohttp import ClientSession, TCPConnector

# ----------------------------------------
# Parámetros por defecto
# ----------------------------------------
DEFAULT_MAX_CONCURRENT = 100
DEFAULT_IP_INTERVAL = 1000  # solo para scraper de defunciones

# ----------------------------------------
# Helpers y Caching
# ----------------------------------------
@st.cache_data
def load_data(uploaded_file):
    """Carga un CSV o Excel y devuelve un DataFrame."""
    if uploaded_file.name.lower().endswith('.csv'):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)


def generate_ip() -> str:
    """Genera una dirección IP aleatoria válida."""
    return f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"

# ----------------------------------------
# Scraper 1: Defunciones Registraduría con progreso
# ----------------------------------------
async def scraper_defunciones(nuips: list[str], max_concurrent: int, ip_interval: int, url: str, progress_bar, progress_label) -> pd.DataFrame:
    """Scraper asíncrono para verificar vigencia en Registraduría con barra de progreso y texto."""
    semaphore = asyncio.Semaphore(max_concurrent)
    async with ClientSession() as session:
        async def limited_fetch(nuip: str, ip: str):
            async with semaphore:
                payload = {"nuip": nuip, "ip": ip}
                try:
                    async with session.post(url, json=payload, timeout=10) as resp:
                        data = await resp.json()
                        return {"Documento": nuip, "Vigencia": data.get("vigencia", "No disponible")}
                except Exception:
                    return {"Documento": nuip, "Vigencia": "Error"}

        tasks = []
        ip_actual = generate_ip()
        for i, nuip in enumerate(nuips):
            if i % ip_interval == 0:
                ip_actual = generate_ip()
            tasks.append(limited_fetch(str(nuip), ip_actual))

        total = len(tasks)
        resultados = []
        for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
            res = await coro
            resultados.append(res)
            # Actualiza barra y etiqueta
            fraction = idx / total
            progress_bar.progress(fraction)
            progress_label.text(f"{idx} de {total} ({fraction:.1%})")
    return pd.DataFrame(resultados)

# ----------------------------------------
# Scraper 2: Morosidad Judicial con progreso
# ----------------------------------------
async def scraper_deudores(nuips: list[str], max_concurrent: int, url: str, progress_bar, progress_label) -> pd.DataFrame:
    """Scraper asíncrono para consultar morosidad en Rama Judicial con barra de progreso y texto."""
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = TCPConnector(limit_per_host=max_concurrent)
    async with ClientSession(connector=connector) as session:
        async def limited_fetch(doc: str):
            async with semaphore:
                payload = {"Documento": doc}
                try:
                    async with session.post(url, json=payload, timeout=30) as resp:
                        if resp.status == 200:
                            json_data = await resp.json()
                            total = json_data.get("Total", 0)
                            data = json_data.get("Data", [])
                            if total > 0 and data:
                                sancionado = data[0].get("Sancionado")
                                estado = "Moroso"
                            else:
                                sancionado = None
                                estado = "No moroso"
                            return {"Documento": doc, "Sancionado": sancionado, "Estado": estado}
                        else:
                            return {"Documento": doc, "Sancionado": None, "Estado": "Error"}
                except Exception:
                    return {"Documento": doc, "Sancionado": None, "Estado": "Error"}

        tasks = [limited_fetch(str(d)) for d in nuips]
        total = len(tasks)
        resultados = []
        for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
            res = await coro
            resultados.append(res)
            fraction = idx / total
            progress_bar.progress(fraction)
            progress_label.text(f"{idx} de {total} ({fraction:.1%})")

    return pd.DataFrame(resultados)

# ----------------------------------------
# Interfaz Streamlit
# ----------------------------------------
def main():
    st.set_page_config(page_title="Multi-Scraper", layout="wide")
    st.title("📡 Multi-Scraper")
    st.markdown(
        "Sube un archivo CSV o Excel con una sola columna de documentos para consultar en los diferentes servicios."
    )

    uploaded = st.file_uploader("Selecciona CSV o XLSX", type=["csv", "xlsx"])
    if not uploaded:
        st.info("Espera a que subas tu archivo.")
        return

    df = load_data(uploaded)
    if df.shape[1] != 1:
        st.error("El archivo debe tener exactamente UNA columna.")
        return

    nuips = df.iloc[:, 0].astype(str).tolist()

    # Sidebar: selección de scraper
    with st.sidebar:
        st.header("Opciones")
        scraper = st.selectbox("Selecciona scraper:", [
            "Defunciones Registraduría", "Morosidad Judicial"
        ])
        ejecutar = st.button("Iniciar Scraping")

    if ejecutar:
        start = perf_counter()
        # Inicializa barra y etiqueta
        progress_bar = st.progress(0)
        progress_label = st.empty()
        with st.spinner("Ejecutando scraping..."):
            if scraper == "Defunciones Registraduría":
                df_res = asyncio.run(
                    scraper_defunciones(
                        nuips,
                        DEFAULT_MAX_CONCURRENT,
                        DEFAULT_IP_INTERVAL,
                        "https://defunciones.registraduria.gov.co:8443/VigenciaCedula/consulta",
                        progress_bar,
                        progress_label,
                    )
                )
            else:
                df_res = asyncio.run(
                    scraper_deudores(
                        nuips,
                        DEFAULT_MAX_CONCURRENT,
                        "https://cobrocoactivo.ramajudicial.gov.co/Home/Bdme_Read",
                        progress_bar,
                        progress_label,
                    )
                )
        elapsed = perf_counter() - start

        # Ocultar barra y etiqueta
        progress_bar.empty()
        progress_label.empty()

        st.success(f"¡Completado en {elapsed:.1f} segundos!")
        st.dataframe(df_res)

        csv_bytes = df_res.to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇ Descargar resultados",
            csv_bytes,
            file_name=f"resultados_{scraper.replace(' ', '_')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
