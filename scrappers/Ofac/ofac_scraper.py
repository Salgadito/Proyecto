import pandas as pd
import re
from typing import List


class SDNChecker:
    """
    Clase para verificar si documentos están reportados en lista SDN (OFAC).
    """

    def __init__(self, sdn_path: str = "sdn.csv") -> None:
        """
        Parameters
        ----------
        sdn_path : str
            Ruta al archivo CSV de la lista SDN (sin procesar).
        """
        self.sdn_path = sdn_path
        self.sdn_cedulas = set()

    def _leer_y_extraer(self) -> None:
        """
        Lee el archivo y extrae cédulas únicas desde la columna Remarks.
        """
        df = pd.read_csv(
            self.sdn_path,
            header=None,
            sep=',',
            quotechar='"',
            encoding='utf-8',
            on_bad_lines='skip'
        )
        # Renombrar columnas
        column_names = [
            'ent_num', 'SDN_Name', 'SDN_Type', 'Program', 'Title',
            'Call_Sign', 'Vess_Type', 'Tonnage', 'GRT', 'Vess_Flag',
            'Vess_Owner', 'Remarks'
        ]
        for i, col in enumerate(column_names):
            if i < len(df.columns):
                df.rename(columns={i: col}, inplace=True)

        # Extraer cédulas
        def extraer_cedulas(texto):
            if pd.isna(texto): return []
            return re.findall(r'Cedula\s+No\.?\s*(\d{7,11})', texto, re.IGNORECASE)

        df['Cedulas_Extraidas'] = df['Remarks'].apply(extraer_cedulas)
        cedulas = df['Cedulas_Extraidas'].explode().dropna().unique()
        self.sdn_cedulas = set(cedulas)

    def run(self, nuips: List[str], progress_bar=None, progress_label=None) -> pd.DataFrame:
        """
        Compara lista de cédulas con base SDN.

        Parameters
        ----------
        nuips : List[str]
            Lista de cédulas a verificar.
        progress_bar : st.progress (opcional)
            Barra de progreso de Streamlit.
        progress_label : st.empty (opcional)
            Etiqueta de progreso de Streamlit.

        Returns
        -------
        pd.DataFrame
            Con columnas ['Documento', 'Estado'], donde Estado puede ser 'Reportado'.
        """
        self._leer_y_extraer()
        resultados = []
        total = len(nuips)

        for idx, doc in enumerate(nuips, start=1):
            estado = "Reportado" if doc in self.sdn_cedulas else "No aplica"
            resultados.append({"Documento": doc, "Estado": estado})

            if progress_bar:
                progress_bar.progress(idx / total)
            if progress_label:
                progress_label.text(f"{idx} de {total}")

        return pd.DataFrame(resultados)
