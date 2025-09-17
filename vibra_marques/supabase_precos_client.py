"""
supabase_precos_client.py
-------------------------
Módulo de integração com Supabase para salvar preços de combustíveis.

⚡ Funcionalidades:
- Faz UPSERT na tabela `precos_combustiveis`
  (usa chave única data_coleta + empresa).
- Aplica regra gasolina_grid -> gasolina_aditivada.
- Permite incluir diesel_s10_aditivado.
- Inclui GET de conferência.
"""

from dotenv import load_dotenv
import os
import requests
import time
from datetime import date

TABLE = "precos_combustiveis"
TIMEOUT = 30


# ------------------- Funções internas -------------------
def _env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Variável de ambiente ausente: {name}")
    return val

def _normalize_payload(precos: dict) -> dict:
    payload = dict(precos)

    # normalizar data
    if isinstance(payload.get("data_coleta"), date):
        payload["data_coleta"] = payload["data_coleta"].isoformat()

    # gasolina_grid -> gasolina_aditivada
    if "gasolina_grid" in payload and payload["gasolina_grid"] is not None:
        payload["gasolina_aditivada"] = payload["gasolina_grid"]
        payload.pop("gasolina_grid", None)

    return payload


# ------------------- Core -------------------
def upsert_precos(precos: dict, return_representation: bool = False) -> tuple[int, str]:
    """
    UPSERT na tabela precos_combustiveis.
    return_representation=True -> retorna o JSON gravado.
    """
    load_dotenv()
    SUPABASE_URL = _env("SUPABASE_URL").rstrip("/")
    SUPABASE_KEY = _env("SUPABASE_KEY")
    payload = _normalize_payload(precos)

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=data_coleta,empresa"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates," +
                  ("return=representation" if return_representation else "return=minimal"),
    }

    # retry simples
    for i in range(3):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
            if resp.status_code in (200, 201):
                return resp.status_code, resp.text or ""
            if resp.status_code >= 500:  # erro de servidor, tenta de novo
                time.sleep(1.5 * (i + 1))
                continue
            return resp.status_code, resp.text
        except requests.RequestException as e:
            if i == 2:
                return 0, f"Erro de rede: {e}"
            time.sleep(1.5 * (i + 1))
    return 0, "Erro desconhecido"

def get_precos(empresa: str, limit: int = 5) -> tuple[int, str]:
    """
    Consulta últimos registros dessa empresa.
    """
    load_dotenv()
    SUPABASE_URL = _env("SUPABASE_URL").rstrip("/")
    SUPABASE_KEY = _env("SUPABASE_KEY")

    params = f"empresa=eq.{requests.utils.quote(empresa)}&order=data_coleta.desc&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?{params}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    resp = requests.get(url, headers=headers, timeout=TIMEOUT)
    return resp.status_code, resp.text


# ------------------- Exemplo -------------------
if __name__ == "__main__":
    precos_do_dia = {
        "data_coleta": "2025-09-02",
        "empresa": "VIBRA MARQUES",
        "gasolina_comum": 5.4058,
        "gasolina_grid": 5.541,   # será convertido para gasolina_aditivada
        "etanol_hidratado": 4.2442,
        "diesel_s10": 5.5501,
        "diesel_s10_aditivado": None
    }

    # UPSERT
    status, body = upsert_precos(precos_do_dia, return_representation=True)
    print("📡 UPSERT Status:", status)
    print("📄 UPSERT Resposta:", body if body else "(vazio)")

    # GET de conferência
    status_get, body_get = get_precos(precos_do_dia["empresa"], limit=5)
    print("📡 GET Status:", status_get)
    print("📄 GET Resposta:", body_get if body_get else "(vazio)")