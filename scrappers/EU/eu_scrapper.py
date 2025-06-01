

# -*- coding: utf-8 -*-
"""
Clase modular para verificar cédulas/documentos en la lista de sanciones de la UE,
buscando cada documento (ignorando ceros a la izquierda en la entrada)
en la columna 'Iden_number' de la lista de la UE.
La lista de la UE permite ceros a la izquierda en sus 'Iden_number'.
Los datos de la lista de la UE se cargan y preparan una vez.
Optimizada para mayor velocidad en la consulta.
"""

import pandas as pd
import re
from typing import List, Dict, Union

class UniversalModularEUChecker:
    """
    Clase para verificar si documentos están reportados en la lista de sanciones de la UE.
    Carga la lista de la UE una vez. Luego, para cada documento de entrada,
    (ignorando sus propios ceros a la izquierda), lo busca en la columna 'Iden_number'
    del archivo de la UE. La búsqueda permite que los números en la lista de la UE
    tengan ceros a la izquierda. Devuelve información detallada de las coincidencias.
    """

    def __init__(self, eu_list_path: str = "20250522-FULL-1_0.csv") -> None:
        """
        Inicializa el verificador cargando y preparando los datos de la lista de la UE.

        Parameters
        ----------
        eu_list_path : str
            Ruta al archivo CSV de la lista de sanciones de la UE.
            Se espera que el separador sea ';'
        """
        self.eu_list_path: str = eu_list_path
        self.df_eu: pd.DataFrame = self._prepare_eu_list_dataframe()

    def _prepare_eu_list_dataframe(self) -> pd.DataFrame:
        """
        Carga el archivo de la lista de la UE desde la ruta especificada y lo prepara.
        Asegura que las columnas clave para la búsqueda y los resultados existan y sean de tipo string.

        Returns
        -------
        pd.DataFrame
            DataFrame de Pandas con los datos de la lista de la UE procesados.

        Raises
        ------
        FileNotFoundError
            Si el archivo de la UE no se encuentra en la ruta especificada.
        ValueError
            Si la columna esencial 'Iden_number' no se encuentra.
        """
        try:
            df = pd.read_csv(
                self.eu_list_path,
                sep=';',
                engine='python', # Como en el script original
                dtype=str, # Leer todas las columnas como string inicialmente
                keep_default_na=False, # Tratar strings vacíos como tal, no NaN
                na_values=[''] # Definir qué se considera NA si es necesario explícitamente
            )
        except FileNotFoundError:
            raise FileNotFoundError(f"Archivo de la lista UE no encontrado en la ruta: {self.eu_list_path}")
        except Exception as e:
            raise ValueError(f"Error al leer el archivo CSV de la UE: {e}")


        # Columnas esenciales para la búsqueda y obtención de resultados
        # Nombres tomados del script de ejemplo del usuario
        essential_columns = {
            'Iden_number': True, # Esencial para la búsqueda
            'Naal_wholename': False,
            'Subject_type': False,
            'Entity_remark': False,
            'EU_ref_num': False,
            'Iden_programme': False
        }

        for col_name, is_critical in essential_columns.items():
            if col_name not in df.columns:
                if is_critical:
                    raise ValueError(f"La columna '{col_name}' es esencial y no se encontró en el archivo de la UE.")
                else:
                    df[col_name] = "" # Inicializa como columna de cadenas vacías si falta
            # Asegurar que sea de tipo string y reemplazar NaNs verdaderos (si los hay después de dtype=str y na_values) por ""
            df[col_name] = df[col_name].fillna("").astype(str).str.strip()
            
        return df

    def run(self, documentos_a_buscar: List[str], progress_bar=None, progress_label=None) -> pd.DataFrame:
        """
        Compara una lista de documentos con la base de datos de la UE cargada.
        Para cada documento de entrada, se eliminan los ceros a la izquierda.
        Luego, se busca este número en la columna 'Iden_number' de la lista de la UE.
        El patrón de búsqueda permite que el 'Iden_number' en la UE tenga ceros a la izquierda
        y que el número esté seguido por un carácter no numérico o el final de la cadena.

        Parameters
        ----------
        documentos_a_buscar : List[str]
            Lista de números de documento (como strings) para verificar.
        progress_bar : st.progress (opcional)
            Barra de progreso de Streamlit para visualización.
        progress_label : st.empty (opcional)
            Etiqueta de texto de Streamlit para mostrar el progreso.

        Returns
        -------
        pd.DataFrame
            Un DataFrame con los resultados de la búsqueda. Las columnas son:
            ['Documento', 'Iden_number_UE', 'Nombre_UE', 'Tipo_UE', 
             'Comentarios_UE', 'ref_num_UE', 'Iden_programme_UE'].
            Si un documento tiene múltiples coincidencias, se genera una fila por cada una.
        """
        resultados = []
        total_documentos = len(documentos_a_buscar)

        output_columns = [
            'Documento', 'Iden_number_UE', 'Nombre_UE', 'Tipo_UE',
            'Comentarios_UE', 'ref_num_UE', 'Iden_programme_UE'
        ]

        if total_documentos == 0:
            return pd.DataFrame(columns=output_columns)
        
        docs_to_process = []
        for doc in documentos_a_buscar:
            original_doc = str(doc) 
            doc_sin_ceros = str(doc).lstrip('0').strip()
            if doc_sin_ceros: 
                 docs_to_process.append({'original': original_doc, 'sin_ceros': doc_sin_ceros})

        unique_doc_strs_sin_ceros_to_search = sorted(list(set(d['sin_ceros'] for d in docs_to_process)))

        if not unique_doc_strs_sin_ceros_to_search:
            for i, original_doc_input in enumerate(documentos_a_buscar):
                resultados.append({
                    'Documento': str(original_doc_input), # Cambio aquí
                    'Iden_number_UE': "Sin coincidencias",
                    'Nombre_UE': "Sin coincidencias",
                    'Tipo_UE': "Sin coincidencias",
                    'Comentarios_UE': "Sin coincidencias",
                    'ref_num_UE': "Sin coincidencias",
                    'Iden_programme_UE': "Sin coincidencias"
                })
                if progress_bar: progress_bar.progress((i + 1) / total_documentos)
                if progress_label: progress_label.text(f"Procesando {i + 1} de {total_documentos}")
            return pd.DataFrame(resultados)

        escaped_unique_docs_sin_ceros = [re.escape(doc_str) for doc_str in unique_doc_strs_sin_ceros_to_search]
        
        if len(escaped_unique_docs_sin_ceros) == 1:
            combined_terms_pattern = escaped_unique_docs_sin_ceros[0]
        else:
            combined_terms_pattern = r"(?:" + r"|".join(escaped_unique_docs_sin_ceros) + r")"
            
        combined_regex_pattern_text = rf"\b0*{combined_terms_pattern}(?:\D|$)"
        
        relevant_eu_mask = self.df_eu['Iden_number'].str.contains(combined_regex_pattern_text, regex=True, na=False)
        relevant_eu_df = self.df_eu[relevant_eu_mask]

        if relevant_eu_df.empty:
            for i, original_doc_input in enumerate(documentos_a_buscar):
                resultados.append({
                    'Documento': str(original_doc_input), # Cambio aquí
                    'Iden_number_UE': "Sin coincidencias",
                    'Nombre_UE': "Sin coincidencias",
                    'Tipo_UE': "Sin coincidencias",
                    'Comentarios_UE': "Sin coincidencias",
                    'ref_num_UE': "Sin coincidencias",
                    'Iden_programme_UE': "Sin coincidencias"
                })
                if progress_bar: progress_bar.progress((i + 1) / total_documentos)
                if progress_label: progress_label.text(f"Procesando {i + 1} de {total_documentos}")
            return pd.DataFrame(resultados)
            
        individual_patterns = {
            doc_str: re.compile(rf"\b0*{re.escape(doc_str)}(?:\D|$)")
            for doc_str in unique_doc_strs_sin_ceros_to_search
        }

        for idx, original_doc_input_val in enumerate(documentos_a_buscar, start=1):
            original_doc_str = str(original_doc_input_val)
            elemento_sin_ceros_str = original_doc_str.lstrip('0').strip()
            
            current_match_found = False
            if elemento_sin_ceros_str and elemento_sin_ceros_str in individual_patterns:
                current_pattern = individual_patterns[elemento_sin_ceros_str]
                
                coincidencias_df = relevant_eu_df[
                    relevant_eu_df['Iden_number'].str.contains(current_pattern, regex=True, na=False)
                ]
                
                if not coincidencias_df.empty:
                    current_match_found = True
                    for record in coincidencias_df.to_dict('records'):
                        resultados.append({
                            'Documento': original_doc_str, # Cambio aquí
                            'Iden_number_UE': record.get('Iden_number', "N/A"),
                            'Nombre_UE': record.get('Naal_wholename', "N/A"),
                            'Tipo_UE': record.get('Subject_type', "N/A"),
                            'Comentarios_UE': record.get('Entity_remark', "N/A"),
                            'ref_num_UE': record.get('EU_ref_num', "N/A"),
                            'Iden_programme_UE': record.get('Iden_programme', "N/A")
                        })
            
            if not current_match_found:
                resultados.append({
                    'Documento': original_doc_str, # Cambio aquí
                    'Iden_number_UE': "Sin coincidencias",
                    'Nombre_UE': "Sin coincidencias",
                    'Tipo_UE': "Sin coincidencias",
                    'Comentarios_UE': "Sin coincidencias",
                    'ref_num_UE': "Sin coincidencias",
                    'Iden_programme_UE': "Sin coincidencias"
                })
            
            if progress_bar:
                progress_bar.progress(idx / total_documentos)
            if progress_label:
                progress_label.text(f"Procesando {idx} de {total_documentos}")
        
        return pd.DataFrame(resultados, columns=output_columns)