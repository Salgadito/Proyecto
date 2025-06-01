# -*- coding: utf-8 -*-
"""
Clase modular para verificar cédulas/documentos en la lista SDN (OFAC)
buscando cada documento de entrada como una palabra completa en 'Remarks'
y devolviendo detalles de las coincidencias.
Los datos de la lista SDN se cargan y preparan una vez.
Optimizada para mayor velocidad en la consulta.
"""

import pandas as pd
import re
from typing import List, Dict, Union

class UniversalModularSDNChecker:
    """
    Clase para verificar si documentos están reportados en lista SDN (OFAC).
    Carga la lista SDN una vez. Luego, para cada documento de entrada,
    lo busca como una palabra completa en la columna 'Remarks' del archivo SDN
    y devuelve información detallada de las coincidencias.
    """

    def __init__(self, sdn_path: str = "sdn.csv") -> None:
        """
        Inicializa el verificador cargando y preparando los datos de la lista SDN.

        Parameters
        ----------
        sdn_path : str, opcional
            Ruta al archivo CSV de la lista SDN (sin procesar).
            Por defecto es "sdn.csv".
        """
        self.sdn_path: str = sdn_path
        self.df_sdn: pd.DataFrame = self._prepare_sdn_dataframe()

    def _prepare_sdn_dataframe(self) -> pd.DataFrame:
        """
        Carga el archivo SDN desde la ruta especificada, lo procesa y lo prepara.
        Esto incluye leer el CSV, renombrar columnas y asegurar que las columnas
        clave ('Remarks', 'SDN_Name', 'SDN_Type') existan y sean de tipo string.

        Returns
        -------
        pd.DataFrame
            DataFrame de Pandas con los datos de la lista SDN procesados.

        Raises
        ------
        FileNotFoundError
            Si el archivo SDN no se encuentra en la ruta especificada.
        ValueError
            Si alguna columna esencial después del renombrado (como 'Remarks')
            no se encuentra o si hay problemas con los datos.
        """
        try:
            df = pd.read_csv(
                self.sdn_path,
                header=None,
                sep=',',
                quotechar='"',
                encoding='utf-8',
                on_bad_lines='skip'
            )
        except FileNotFoundError:
            raise FileNotFoundError(f"Archivo SDN no encontrado en la ruta: {self.sdn_path}")

        sdn_column_names = [
            'ent_num', 'SDN_Name', 'SDN_Type', 'Program', 'Title',
            'Call_Sign', 'Vess_Type', 'Tonnage', 'GRT', 'Vess_Flag',
            'Vess_Owner', 'Remarks'
        ]

        cols_to_rename = {
            i: col_name for i, col_name in enumerate(sdn_column_names) if i < len(df.columns)
        }
        df.rename(columns=cols_to_rename, inplace=True)

        for col_name in ['Remarks', 'SDN_Name', 'SDN_Type']:
            if col_name not in df.columns:
                if col_name == 'Remarks':
                    raise ValueError(f"La columna '{col_name}' es esencial y no se encontró en el archivo SDN.")
                else:
                    df[col_name] = ""  # Inicializa como columna de cadenas vacías si falta
            df[col_name] = df[col_name].astype(str) # Asegura que sea de tipo string
        
        # Asegurar que otras columnas potencialmente usadas también sean strings si existen
        for col_name in ['Program', 'Title']:
             if col_name in df.columns:
                 df[col_name] = df[col_name].astype(str)
             else: # Si no existen, las crea vacías para evitar errores de .get() si se usaran
                 df[col_name] = ""


        return df

    def run(self, documentos_a_buscar: List[str], progress_bar=None, progress_label=None) -> pd.DataFrame:
        """
        Compara una lista de documentos con la base de datos SDN cargada.
        Para cada documento, lo busca como una palabra completa dentro de la
        columna 'Remarks' del DataFrame SDN.

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
            ['Documento', 'Nombre_OFAC', 'Tipo_OFAC', 'Comentarios_OFAC'].
            Si un documento tiene múltiples coincidencias, se genera una fila por cada una.
        """
        resultados = []
        total_documentos = len(documentos_a_buscar)

        if total_documentos == 0:
            return pd.DataFrame(columns=['Documento', 'Nombre_OFAC', 'Tipo_OFAC', 'Comentarios_OFAC'])

        # 1. Obtener strings de documentos únicos para construir el filtro combinado inicial.
        # Filtrar cadenas vacías después de la conversión a string.
        unique_doc_strs_to_search = sorted(list(set(str(d).strip() for d in documentos_a_buscar if str(d).strip())))

        # Si no hay strings de documentos válidos para buscar, todos los originales resultan en "Sin coincidencias".
        if not unique_doc_strs_to_search:
            for i, original_doc_input in enumerate(documentos_a_buscar):
                resultados.append({
                    'Documento': str(original_doc_input),
                    'Nombre_OFAC': "Sin coincidencias",
                    'Tipo_OFAC': "Sin coincidencias",
                    'Comentarios_OFAC': "Sin coincidencias"
                })
                if progress_bar: progress_bar.progress((i + 1) / total_documentos)
                if progress_label: progress_label.text(f"Procesando {i + 1} de {total_documentos}")
            return pd.DataFrame(resultados)

        # 2. Crear un único patrón regex para encontrar *cualquiera* de los documentos únicos.
        escaped_unique_docs = [re.escape(doc_str) for doc_str in unique_doc_strs_to_search]
        combined_regex_pattern = r"\b(?:" + r"|".join(escaped_unique_docs) + r")\b"
        
        # 3. Filtrar df_sdn para obtener solo filas que potencialmente contengan alguno de los términos.
        relevant_sdn_mask = self.df_sdn['Remarks'].str.contains(combined_regex_pattern, regex=True, na=False)
        relevant_sdn_df = self.df_sdn[relevant_sdn_mask]

        # Si ninguna fila de SDN es relevante, todos los documentos serán "Sin coincidencias".
        if relevant_sdn_df.empty:
            for i, original_doc_input in enumerate(documentos_a_buscar):
                resultados.append({
                    'Documento': str(original_doc_input),
                    'Nombre_OFAC': "Sin coincidencias",
                    'Tipo_OFAC': "Sin coincidencias",
                    'Comentarios_OFAC': "Sin coincidencias"
                })
                if progress_bar: progress_bar.progress((i + 1) / total_documentos)
                if progress_label: progress_label.text(f"Procesando {i + 1} de {total_documentos}")
            return pd.DataFrame(resultados)
            
        # 4. Pre-compilar patrones regex individuales para la búsqueda exacta posterior.
        individual_patterns = {
            doc_str: re.compile(rf"\b{re.escape(doc_str)}\b")
            for doc_str in unique_doc_strs_to_search
        }

        # 5. Iterar sobre la lista original de documentos_a_buscar.
        for idx, original_doc_input in enumerate(documentos_a_buscar, start=1):
            elemento_str = str(original_doc_input).strip()
            
            current_match_found = False
            if elemento_str and elemento_str in individual_patterns:
                current_pattern = individual_patterns[elemento_str]
                
                coincidencias_df = relevant_sdn_df[
                    relevant_sdn_df['Remarks'].str.contains(current_pattern, regex=True, na=False)
                ]
                
                if not coincidencias_df.empty:
                    current_match_found = True
                    for record in coincidencias_df.to_dict('records'):
                        resultados.append({
                            'Documento': elemento_str, # Usar el string del documento original buscado
                            'Nombre_OFAC': record.get('SDN_Name', "N/A"),
                            'Tipo_OFAC': record.get('SDN_Type', "N/A"),
                            'Comentarios_OFAC': record.get('Remarks', "") # Remarks debería existir
                        })
            
            if not current_match_found:
                resultados.append({
                    'Documento': str(original_doc_input), # Mantener el input original si no hubo match o fue inválido
                    'Nombre_OFAC': "Sin coincidencias",
                    'Tipo_OFAC': "Sin coincidencias",
                    'Comentarios_OFAC': "Sin coincidencias"
                })
            
            if progress_bar:
                progress_bar.progress(idx / total_documentos)
            if progress_label:
                progress_label.text(f"Procesando {idx} de {total_documentos}")
        
        return pd.DataFrame(resultados)