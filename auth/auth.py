import streamlit as st
import hashlib
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Función para hashear contraseñas con SHA-256
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Cargar usuarios desde .env
def load_users():
    users = {}
    # Forzar la recarga de las variables de entorno para reflejar cambios en .env
    load_dotenv(override=True)
    for key, value in os.environ.items():
        if key.startswith("USER_"):
            username = key.replace("USER_", "")
            users[username] = value
    return users

# USERS se carga inicialmente, pero load_users() se llama de nuevo
# antes de operaciones críticas para intentar obtener la data más reciente.
USERS = load_users()

def logout():
    """
    Cierra la sesión del usuario actual.
    Actualiza el estado de la sesión y recarga la aplicación.
    """
    st.session_state["authenticated"] = False
    st.session_state["user"] = None
    # st.sidebar.success("Has cerrado sesión correctamente.") # Mensaje opcional, puede ser en la página principal
    st.rerun()

def login():
    """
    Maneja la interfaz y lógica de inicio de sesión.
    Si el usuario ya está autenticado, no muestra nada aquí (la app principal se encarga).
    """
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
        st.session_state["user"] = None

    # Si el usuario ya está autenticado, la función login no necesita mostrar nada más.
    # El control para cerrar sesión estará en la app principal.
    if st.session_state.get("authenticated"):
        return

    # Mostrar el formulario de inicio de sesión en la sidebar
    st.sidebar.image("Logo.jpg", width=150) # 👈 AQUÍ SE AGREGA EL LOGO
    st.sidebar.title("🔐 Iniciar Sesión")

    username = st.sidebar.text_input("Usuario", key="login_username_main") # Clave única
    password = st.sidebar.text_input("Contraseña", type="password", key="login_password_main") # Clave única

    if st.sidebar.button("Entrar", key="login_button_main", type="primary"): # Clave única
        # Recargar usuarios para asegurar que tenemos la lista más actualizada
        current_users = load_users()
        hashed_input_password = hash_password(password)

        if current_users.get(username) == hashed_input_password:
            st.session_state["authenticated"] = True
            st.session_state["user"] = username
            # El mensaje de bienvenida es mejor en la app principal tras el rerun.
            st.rerun()
        else:
            st.sidebar.error("Usuario o contraseña incorrectos.")
            st.session_state["authenticated"] = False
            st.session_state["user"] = None
    
    # Dejar que la app principal maneje el st.stop() si no está autenticado.
    # La función login solo provee la UI para intentar loguearse.

def register_user():
    """
    Maneja la interfaz y lógica para registrar nuevos usuarios.
    Solo accesible por el usuario 'admin'.
    Esta función se espera que sea llamada en el cuerpo principal de la página.
    """
    if st.session_state.get("user", "").lower() != "admin":
        st.warning("Acceso restringido. Solo el administrador puede registrar nuevos usuarios.")
        return

    # El logo aquí podría ser redundante si ya está en la cabecera de la app principal.
    # Si es una sección separada de "Panel de Admin", podría tener su propio branding.
    # Por ahora, no se añade aquí para evitar repetición excesiva del logo.

    with st.form("register_form"):
        st.subheader("🧾 Registrar Nuevo Usuario")
        new_user = st.text_input("Nuevo nombre de usuario")
        new_pass = st.text_input("Nueva contraseña (mín. 8 caracteres)", type="password")
        confirm_pass = st.text_input("Confirmar nueva contraseña", type="password")
        
        submitted = st.form_submit_button("Registrar Usuario")

        if submitted:
            if not new_user or not new_pass:
                st.warning("El nombre de usuario y la contraseña son obligatorios.")
                return
            
            if len(new_pass) < 8: # Ejemplo de validación simple
                st.warning("La contraseña debe tener al menos 8 caracteres.")
                return

            if new_pass != confirm_pass:
                st.error("Las contraseñas no coinciden.")
                return

            current_users = load_users() # Cargar usuarios actualizados
            if new_user in current_users:
                st.error(f"El usuario '{new_user}' ya existe.")
                return

            hashed_new_password = hash_password(new_pass)
            # Guardar como USER_nombredeusuario=hash
            # Usar new_user directamente como viene (sensible a mayúsculas/minúsculas)
            # o normalizarlo (ej. new_user.lower() o new_user.upper())
            env_key = f"USER_{new_user}"
            env_entry = f"\n{env_key}={hashed_new_password}"

            try:
                with open(".env", "a") as f:
                    f.write(env_entry)
                
                # Actualizar la variable global USERS en memoria y forzar recarga de dotenv
                USERS[new_user] = hashed_new_password
                load_dotenv(override=True) 
                
                st.success(f"Usuario '{new_user}' registrado correctamente. El cambio ha sido aplicado.")
                st.info("Es posible que necesites recargar la página o que la aplicación se recargue para que todos los cambios de entorno sean visibles globalmente si la app es compleja.")

            except Exception as e:
                st.error(f"Error al guardar el nuevo usuario en el archivo .env: {e}")