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
    for key, value in os.environ.items():
        if key.startswith("USER_"):
            username = key.replace("USER_", "")
            users[username] = value
    return users

USERS = load_users()

def login():
    st.sidebar.title("🔐 Iniciar Sesión")
    
    if st.session_state.get("authenticated"):
        st.sidebar.success(f"👤 Usuario: {st.session_state['user']}")
        if st.sidebar.button("Cerrar sesión"):
            st.session_state["authenticated"] = False
            st.session_state["user"] = None
            st.rerun()
        return

    username = st.sidebar.text_input("Usuario")
    password = st.sidebar.text_input("Contraseña", type="password")
    
    if st.sidebar.button("Entrar"):
        hashed_input = hash_password(password)
        if USERS.get(username) == hashed_input:
            st.session_state["authenticated"] = True
            st.session_state["user"] = username
            st.success(f"Bienvenido, {username}")
            st.rerun()
        else:
            st.error("Credenciales incorrectas")
def register_user():
    st.subheader("🧾 Registro de Usuario (solo Admin)")
    
    if st.session_state.get("user") != "admin":
        st.warning("Solo el administrador puede registrar nuevos usuarios.")
        return

    new_user = st.text_input("Nuevo usuario")
    new_pass = st.text_input("Nueva contraseña", type="password")

    if st.button("Registrar"):
        if not new_user or not new_pass:
            st.warning("Usuario y contraseña son obligatorios.")
            return

        hashed = hash_password(new_pass)
        entry = f"\nUSER_{new_user}={hashed}"

        try:
            with open(".env", "a") as f:
                f.write(entry)
            st.success(f"Usuario '{new_user}' registrado correctamente.")
        except Exception as e:
            st.error(f"Error al guardar usuario: {e}")

