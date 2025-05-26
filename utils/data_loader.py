import pandas as pd
import streamlit as st


@st.cache_data
def load_data(uploaded_file) -> pd.DataFrame:
    """
    Carga un CSV o Excel y devuelve un DataFrame.

    Parameters
    ----------
    uploaded_file : UploadedFile
        Archivo subido en Streamlit (csv o xlsx).

    Returns
    -------
    pd.DataFrame
        Datos cargados.
    """
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)
