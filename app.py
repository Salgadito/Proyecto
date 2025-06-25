import asyncio
from time import perf_counter
import pandas as pd
import streamlit as st
import inspect
import traceback

# --- Asunciones sobre tus módulos ---
# Asegúrate de que estas importaciones sean correctas y los archivos/directorios existan.
# Es especialmente importante que SCRAPERS y SCRAPER_CLASSES estén bien definidos.
from config.scrappers_config import SCRAPERS
from scrappers import SCRAPER_CLASSES
from utils.data_loader import load_data
from auth.auth import login, logout, register_user

# --- Configuración de la Página de Streamlit ---
st.set_page_config(
    page_title="KnowMe",
    layout="wide",
    page_icon="Logo.jpg", # Tu logo
    initial_sidebar_state="expanded"
)

# --- Inyección de CSS y Font Awesome para un look moderno ---
st.markdown("""
<style>
    /* Importar Font Awesome */
    @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css');
    
    /* Mejoras visuales generales */
    .stApp {
        background-color: #f0f2f5; /* Un fondo gris claro para la app */
    }
    
    /* Estilo para los botones principales */
    .stButton > button {
        border-radius: 8px;
        border: 1px solid transparent;
        transition: background-color 0.2s, border-color 0.2s;
    }
    
    /* Tarjetas de Módulos */
    [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="stVerticalBlock"] {
        border-radius: 10px;
        padding: 1.5rem;
        background-color: #ffffff;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: box-shadow 0.3s ease-in-out;
    }
    /* Efecto hover para las tarjetas */
    [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="stVerticalBlock"]:hover {
        box-shadow: 0 8px 12px rgba(0,0,0,0.15);
    }
</style>
""", unsafe_allow_html=True)


def _display_sidebar():
    """Muestra la barra lateral con opciones de sesión y admin."""
    with st.sidebar:
        st.image("Logo.jpg", use_container_width=True)
        st.header("⚙️ Opciones")
        
        if "user" in st.session_state and st.session_state.get("user") is not None:
            st.success(f"👤 Usuario: **{st.session_state['user']}**")
        
            if str(st.session_state.get("user", "")).lower() == "admin":
                st.markdown("---")
                st.subheader("🛠️ Panel de Admin")
                with st.expander("🧾 Registrar Nuevo Usuario"):
                    register_user()
        
        st.markdown("---")
        if st.button("🚪 Cerrar Sesión", use_container_width=True):
            st.session_state['selected_module'] = None
            logout()
            st.rerun()

def show_module_selection():
    """Muestra una pantalla de bienvenida con tarjetas para cada módulo de consulta."""
    _display_sidebar()

    st.title("Bienvenido a KnowMe")
    st.markdown("#### Selecciona un módulo para iniciar una consulta.")
    st.markdown("---")

    scraper_names = list(SCRAPERS.keys())
    if not scraper_names:
        st.warning("⚠️ No hay módulos de consulta configurados.")
        st.stop()

    # --- Diccionario de Iconos (Personalizable) ---
    # Añade aquí los iconos que prefieras para cada scraper
    scraper_icons = {
        "Procuraduría": "fas fa-gavel",
        "Contraloría": "fas fa-balance-scale",
        "Policía": "fas fa-shield-alt",
        "Ejército": "fas fa-user-shield",
        # Añade más scrapers y sus iconos aquí
    }
    default_icon = "fas fa-search"

    # --- Grid de Tarjetas de Módulos ---
    num_columns = min(len(scraper_names), 3) # Máximo 3 columnas para un buen layout
    cols = st.columns(num_columns)

    for i, name in enumerate(scraper_names):
        with cols[i % num_columns]:
            # Usamos un container para crear el efecto de "tarjeta"
            with st.container(border=True):
                icon_class = scraper_icons.get(name, default_icon)
                st.markdown(f"### <i class='{icon_class}'></i> {name}", unsafe_allow_html=True)
                st.markdown(f"Realiza consultas de documentos en el módulo de **{name}**.")
                
                # Espaciador para empujar el botón al fondo
                st.write("")
                st.write("")

                if st.button("Abrir Módulo", key=f"btn_{name}", use_container_width=True):
                    st.session_state['selected_module'] = name
                    st.rerun()

def show_scraper_page(scraper_name):
    """Muestra la página dedicada a un módulo de scraper individual."""
    _display_sidebar()

    st.title(f"📄 Módulo: {scraper_name}")
    st.markdown(f"Sube un archivo para realizar consultas en **{scraper_name}**.")
    st.markdown("---")

    if st.button("⬅️ Volver al Menú de Módulos"):
        st.session_state['selected_module'] = None
        st.rerun()

    uploaded_file = st.file_uploader(
        "📂 **Sube tu archivo (CSV o XLSX)**",
        type=["csv", "xlsx"],
        help="El archivo debe contener una única columna con los números de documento."
    )

    if not uploaded_file:
        st.info("ℹ️ Por favor, sube un archivo para continuar.")
        st.stop()

    df_base = load_data(uploaded_file)
    if df_base is None or df_base.shape[1] != 1:
        st.error("❌ **Error:** El archivo debe tener exactamente UNA columna. Verifica el formato.")
        st.stop()

    df_base.columns = ["Documento"]
    try:
        df_base["Documento"] = df_base["Documento"].astype(str)
    except Exception as e:
        st.error(f"❌ Error al procesar la columna 'Documento': {e}")
        st.stop()
        
    nuips = df_base["Documento"].tolist()

    st.markdown("---")
    if st.button(f"🚀 Iniciar Consulta en {scraper_name}", type="primary", use_container_width=True):
        
        start_time = perf_counter()
        progress_container = st.container()
        ui_overall_progress_bar = progress_container.progress(0.0, text="Iniciando proceso...")
        ui_detailed_progress_label = progress_container.empty()

        with st.spinner(f"⏳ Ejecutando consulta en {scraper_name}... Por favor, espera."):
            df_result, summary = run_single_scraper(scraper_name, nuips, ui_overall_progress_bar, ui_detailed_progress_label)
        
        elapsed_time = perf_counter() - start_time
        ui_overall_progress_bar.empty()
        ui_detailed_progress_label.empty()

        st.markdown("---")
        if df_result is not None:
            st.success(f"🎉 ¡Proceso completado en {elapsed_time:.2f} segundos!")
            st.info(f"📄 **Resumen:** {summary}")
            
            st.subheader("Resultados de la Consulta:")
            st.dataframe(df_result, use_container_width=True)

            try:
                csv_bytes = df_result.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label=f"⬇️ Descargar resultados como CSV",
                    data=csv_bytes,
                    file_name=f"resultados_{scraper_name.lower().replace(' ', '_')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"❌ Error al generar el archivo CSV para descarga: {e}")
        else:
            st.error(f"El proceso finalizó con errores. {summary}")

def run_single_scraper(scraper_name, nuips, progress_bar, progress_label):
    """Ejecuta un único scraper y devuelve los resultados."""
    cfg = SCRAPERS.get(scraper_name)
    ScraperClass = SCRAPER_CLASSES.get(scraper_name)

    if not ScraperClass or not cfg:
        msg = f"No existe implementación o configuración para '{scraper_name}'."
        return None, msg

    scraper_instance = ScraperClass(**cfg)
    try:
        run_method = scraper_instance.run
        is_async = inspect.iscoroutinefunction(run_method)
        params = inspect.signature(run_method).parameters
        run_args = [nuips]
        
        if "progress_bar" in params and "progress_label" in params:
            run_args.extend([progress_bar, progress_label])

        df_res = asyncio.run(run_method(*run_args)) if is_async else run_method(*run_args)
        
        if "Documento" not in df_res.columns:
            return None, f"El scraper '{scraper_name}' no devolvió la columna 'Documento'."
        
        df_res["Documento"] = df_res["Documento"].astype(str)
        return df_res, f"Consulta en '{scraper_name}' completada exitosamente."

    except Exception as e:
        tb_str = traceback.format_exc()
        error_type = type(e).__name__
        msg = f"Error crítico en '{scraper_name}': {error_type} - {e}."
        print(f"--- TRACEBACK ERROR: {scraper_name} ---\n{tb_str}\n---")
        return None, msg

def main():
    """Función principal que gestiona la autenticación y la navegación."""
    if "authenticated" not in st.session_state:
        st.session_state.update({
            "authenticated": False,
            "user": None,
            "selected_module": None
        })

    if not st.session_state["authenticated"]:
        login()
        if not st.session_state["authenticated"]:
            st.stop()
    
    if st.session_state.get('selected_module'):
        show_scraper_page(st.session_state['selected_module'])
    else:
        show_module_selection()


if __name__ == "__main__":
    main()