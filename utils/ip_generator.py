import random


def generate_ip() -> str:
    """
    Genera una dirección IP aleatoria válida v4.
    Evita 0 en el primer y último octeto (como en tu versión original).
    Returns
    -------
    str
        IP en formato 'X.X.X.X'.
    """
    return (
        f"{random.randint(1, 255)}."
        f"{random.randint(0, 255)}."
        f"{random.randint(0, 255)}."
        f"{random.randint(1, 255)}"
    )