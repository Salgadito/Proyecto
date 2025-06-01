import asyncio
from time import perf_counter
import pandas as pd
import streamlit as st
import inspect # Asegúrate de que inspect está importado

# Asumo que estas importaciones son correctas y los archivos existen
from config.scrappers_config import SCRAPERS
from scrappers import SCRAPER_CLASSES # Asegúrate que este mapeo es correcto
from utils.data_loader import load_data
from auth.auth import login, logout # Asumo que tienes una función logout en auth.py
# La función register_user() usualmente se llama desde la lógica de login/registro,
# no directamente en el flujo principal después de un login exitoso.
# Considera integrar la opción de registro dentro de tu función login().

# --- Configuración de la Página ---
st.set_page_config(
    page_title="KnowMe",
    layout="wide",
    page_icon="Logo.jpg", # Asegúrate que Logo.jpg está en el directorio raíz o proporciona la ruta
    initial_sidebar_state="expanded"
)

def show_main_application():
    """Muestra la interfaz principal de la aplicación después de la autenticación."""

    # --- Cabecera ---
    col1, col2 = st.columns([1, 4]) # Ajusta la proporción según el tamaño de tu logo
    with col1:
        st.image("Logo.jpg", width=150) # Ajusta el ancho según sea necesario
    with col2:
        st.title("📡 KnowMe")
        st.markdown(
            "Sube un archivo CSV o Excel con una columna de **Documentos** "
            "para realizar consultas en los servicios seleccionados."
        )
    st.markdown("---") # Separador visual

    # --- Carga de Archivo ---
    uploaded_file = st.file_uploader(
        "📂 Selecciona tu archivo (CSV o XLSX)",
        type=["csv", "xlsx"],
        help="El archivo debe contener una única columna con los números de documento."
    )

    if not uploaded_file:
        st.info("ℹ️ Por favor, sube un archivo para continuar.")
        return

    df_base = load_data(uploaded_file)
    if df_base is None: # Asumiendo que load_data puede devolver None si falla
        st.error("❌ No se pudo cargar el archivo. Verifica el formato.")
        return

    if df_base.shape[1] != 1:
        st.error("❌ El archivo debe tener exactamente UNA columna.")
        return

    df_base.columns = ["Documento"]
    try:
        df_base["Documento"] = df_base["Documento"].astype(str)
    except Exception as e:
        st.error(f"❌ Error al convertir la columna 'Documento' a texto: {e}")
        return
        
    nuips = df_base["Documento"].tolist()

    # --- Sidebar (Opciones) ---
    with st.sidebar:
        st.header("⚙️ Opciones de Consulta")
        if "user" in st.session_state:
            st.success(f"👤 Usuario: **{st.session_state['user']}**")
        
        st.markdown("Selecciona los servicios a consultar:")
        selected_scrapers = [
            name for name in SCRAPERS.keys()
            if st.checkbox(name, key=f"chk_{name}") # Usar nombres más directos
        ]

        st.markdown("---")
        run_button = st.button("🚀 Iniciar Consultas", type="primary", use_container_width=True)
        
        st.markdown("---")
        if st.button("🚪 Cerrar Sesión", use_container_width=True):
            logout() # Llama a la función logout
            st.rerun()


    if not run_button:
        st.caption("Configura las opciones en el panel lateral y presiona 'Iniciar Consultas'.")
        return

    if not selected_scrapers:
        st.warning("⚠️ Debes seleccionar al menos un servicio para consultar.")
        return

    # --- Procesamiento ---
    df_final = df_base.copy()
    start_time = perf_counter()
    
    # Contenedor para el progreso, para poder limpiarlo después
    progress_container = st.container()
    # Esta barra de progreso se usará para el progreso general y 
    # el scraper la usará para actualizar su porcentaje de llenado.
    # El texto de esta barra lo gestiona el bucle principal de app.py.
    ui_overall_progress_bar = progress_container.progress(0.0, text="Iniciando proceso...")
    # Esta etiqueta se usará para los mensajes detallados del scraper (ej: "X de Y procesados").
    ui_detailed_progress_label = progress_container.empty() 

    total_scrapers = len(selected_scrapers)
    results_summary = [] # Para resumir los resultados de cada scraper

    with st.spinner("⏳ Ejecutando consultas... Por favor, espera."):
        for i, choice in enumerate(selected_scrapers, 1):
            scraper_name = choice # Para mayor claridad
            progress_text = f"Consultando {scraper_name}... ({i}/{total_scrapers})"
            
            # Actualiza la barra de progreso principal para indicar qué scraper está comenzando
            ui_overall_progress_bar.progress( (i-1) / total_scrapers, text=progress_text)
            ui_detailed_progress_label.empty() # Limpia la etiqueta de progreso detallado del scraper anterior

            cfg = SCRAPERS.get(scraper_name)
            ScraperClass = SCRAPER_CLASSES.get(scraper_name)

            if ScraperClass is None:
                error_msg = f"❌ No existe implementación para el scraper '{scraper_name}'."
                st.error(error_msg)
                results_summary.append(f"{scraper_name}: Error - {error_msg}")
                continue
            
            if cfg is None: # Verificación adicional por si SCRAPERS no tiene la config
                error_msg = f"❌ No se encontró configuración para '{scraper_name}'."
                st.error(error_msg)
                results_summary.append(f"{scraper_name}: Error - {error_msg}")
                continue

            scraper_instance = ScraperClass(**cfg)
            try:
                run_method = scraper_instance.run
                is_async = inspect.iscoroutinefunction(run_method)
                
                # Inspeccionar la firma del método run
                sig = inspect.signature(run_method)
                params = sig.parameters

                run_args = [nuips] # El argumento 'nuips' siempre se pasa (asumimos)
                
                # Comprobar si el método run espera 'progress_bar' y 'progress_label'
                if "progress_bar" in params and "progress_label" in params:
                    run_args.append(ui_overall_progress_bar)
                    run_args.append(ui_detailed_progress_label)

                if is_async:
                    df_res = asyncio.run(run_method(*run_args))
                else: # Scrapers síncronos
                    df_res = run_method(*run_args)
                
                # --- Procesamiento del df_res ---
                if "Documento" not in df_res.columns:
                    error_msg = f"❌ El scraper '{scraper_name}' no devolvió la columna 'Documento'."
                    st.error(error_msg)
                    results_summary.append(f"{scraper_name}: Error - {error_msg}")
                    continue
                
                # Asegurar que la columna 'Documento' en df_res sea string para el merge
                df_res["Documento"] = df_res["Documento"].astype(str)

                cols_to_rename = {
                    col: f"{scraper_name}_{col}" for col in df_res.columns if col != "Documento"
                }
                df_res = df_res.rename(columns=cols_to_rename)

                df_final = pd.merge(df_final, df_res, on="Documento", how="left")
                results_summary.append(f"{scraper_name}: ✅ Completado")

            except Exception as e:
                # Mostrar un mensaje de error más detallado en la UI y registrar el traceback en la consola
                import traceback
                tb_str = traceback.format_exc()
                error_type = type(e).__name__
                error_msg_display = f"❌ Error crítico al ejecutar '{scraper_name}': {error_type} - {e}. Detalles en la consola."
                st.error(error_msg_display)
                print(f"--- TRACEBACK ERROR: {scraper_name} ---") # Para la consola
                print(tb_str)
                print("--------------------------------------")
                results_summary.append(f"{scraper_name}: Error - {error_type}: {str(e)}")
                continue # Continuar con el siguiente scraper
            
            # Actualiza la barra de progreso principal para indicar que este scraper ha terminado
            ui_overall_progress_bar.progress(i / total_scrapers, text=f"Completado: {scraper_name} ({i}/{total_scrapers})")

    elapsed_time = perf_counter() - start_time
    ui_overall_progress_bar.empty() # Limpiar la barra de progreso principal
    ui_detailed_progress_label.empty() # Limpiar la etiqueta de progreso detallado

    # --- Resultados ---
    st.success(f"🎉 ¡Proceso completado en {elapsed_time:.2f} segundos!")
    
    st.subheader("📄 Resumen de Consultas:")
    for summary in results_summary:
        if "Error" in summary:
            st.warning(summary)
        else:
            st.info(summary)
    st.markdown("---")

    st.subheader("📊 Resultados Combinados:")
    st.dataframe(df_final, use_container_width=True)

    # Convertir a CSV para descarga
    try:
        csv_bytes = df_final.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Descargar resultados como CSV",
            data=csv_bytes,
            file_name="resultados_knowme_combinados.csv",
            mime="text/csv",
            use_container_width=True
        )
    except Exception as e:
        st.error(f"❌ Error al generar el archivo CSV para descarga: {e}")


def main():
    """Función principal para ejecutar la aplicación Streamlit."""
    
    # --- Gestión de Sesión y Autenticación ---
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
        st.session_state["user"] = None # o un valor por defecto

    if not st.session_state["authenticated"]:
        # La función login() debería manejar la UI de inicio de sesión y registro
        # y actualizar st.session_state["authenticated"] y st.session_state["user"]
        login() 
        # Si login() no detiene la ejecución con st.stop() o st.rerun() tras un fallo,
        # podrías necesitar un st.stop() aquí.
        # Sin embargo, es mejor que login() gestione su propio flujo.
        if not st.session_state["authenticated"]: # Re-chequear por si login no hizo st.rerun
            st.stop() # Detiene la ejecución si no está autenticado después de llamar a login
    else:
        # Si está autenticado, muestra la aplicación principal
        show_main_application()

if __name__ == "__main__":
    main()