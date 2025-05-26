# KnowMe

Aplicación Streamlit modular que permite realizar scraping de múltiples servicios de forma concurrente y extensible.

## Estructura

- `app.py`: punto de entrada Streamlit.
- `auth/`: Inicio y registro de usuarios.
- `config/`: configuración de scrappers.
- `scrappers/`: módulos de scraping.
- `utils/`: utilidades comunes.

## Uso

```bash
pip install -r requirements.txt
streamlit run app.py
```
