import asyncio
from time import perf_counter
import pandas as pd
import streamlit as st
import inspect

from config.scrappers_config import SCRAPERS
from scrappers import SCRAPER_CLASSES
from utils.data_loader import load_data
from auth.auth import login, register_user


def main():
    st.set_page_config(page_title="KnowMe", layout="wide",page_icon="Logo.jpg")
    
    # --- Inicio de sesión ---
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        login()
        st.stop()

    register_user()
    st.image("Logo.jpg", width=200)
    st.title("📡 KnowMe")
    st.markdown(
        "Sube un archivo CSV o Excel con una sola columna de Documentos "
        "para consultar en los diferentes servicios."
    )

    uploaded = st.file_uploader("Selecciona CSV o XLSX", type=["csv", "xlsx"])
    if not uploaded:
        st.info("Espera a que subas tu archivo.")
        return

    df_base = load_data(uploaded)
    if df_base.shape[1] != 1:
        st.error("El archivo debe tener exactamente UNA columna.")
        return

    df_base.columns = ["Documento"]
    df_base["Documento"] = df_base["Documento"].astype(str)
    nuips = df_base["Documento"].tolist()

    with st.sidebar:
        st.header("Opciones")
        st.markdown(f"👤 Usuario: `{st.session_state['user']}`")

        selected_scrapers = [
            name for name in SCRAPERS.keys()
            if st.checkbox(f"Opción: {name}")
        ]

        run_button = st.button("Iniciar consultas")

    if not run_button:
        return

    if not selected_scrapers:
        st.warning("Selecciona al menos una opción.")
        return

    df_final = df_base.copy()
    start = perf_counter()
    progress_bar = st.progress(0.0)
    progress_label = st.empty()

    with st.spinner("Ejecutando..."):
        for i, choice in enumerate(selected_scrapers, 1):
            cfg = SCRAPERS[choice]
            ScraperClass = SCRAPER_CLASSES.get(choice)

            if ScraperClass is None:
                st.error(f"No existe implementación para el scrapper '{choice}'.")
                continue

            scraper = ScraperClass(**cfg)
            try:
                #df_res = asyncio.run(scraper.run(nuips, progress_bar, progress_label))
                run_method = scraper.run
                is_async = inspect.iscoroutinefunction(run_method)

                if is_async:
                    df_res = asyncio.run(run_method(nuips, progress_bar, progress_label))
                else:
                    df_res = run_method(nuips, progress_bar, progress_label)
                # Aseguramos que haya una columna 'Documento' para hacer merge
                if "Documento" not in df_res.columns:
                    st.error(f"El scraper '{choice}' no devuelve columna 'Documento'.")
                    continue

                # Renombrar columnas para evitar conflictos
                cols_to_rename = {
                    col: f"{choice}_{col}" for col in df_res.columns if col != "Documento"
                }
                df_res = df_res.rename(columns=cols_to_rename)

                # Hacer merge con el DataFrame base
                df_final = df_final.merge(df_res, on="Documento", how="left")

            except Exception as e:
                st.error(f"Error al ejecutar '{choice}': {e}")
                continue

            progress_bar.progress(i / len(selected_scrapers))
            progress_label.text(f"{i}/{len(selected_scrapers)} completado(s)")

    elapsed = perf_counter() - start
    progress_bar.empty()
    progress_label.empty()

    st.success(f"¡Completado en {elapsed:.1f} segundos!")
    st.dataframe(df_final)

    csv_bytes = df_final.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇ Descargar resultados",
        csv_bytes,
        file_name="resultados_scrapers_combinados.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
