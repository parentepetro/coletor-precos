from dotenv import load_dotenv
import os
import requests
from datetime import date

# -------- Config --------
TABLE = "precos_combustiveis"
TIMEOUT = 30

# -------- Util --------
def _require_env(var_name: str) -> str:
    val = os.getenv(var_name)
    if not val:
        raise RuntimeError(f"Variável de ambiente ausente: {var_name}")
    return val

def _normalize_payload(precos: dict) -> dict:
    payload = dict(precos)

    # data_coleta pode vir como date ou string; normaliza para YYYY-MM-DD
    if isinstance(payload.get("data_coleta"), date):
        payload["data_coleta"] = payload["data_coleta"].isoformat()

    # Regra pedida: gasolina_grid -> gasolina_aditivada
    if "gasolina_grid" in payload and payload["gasolina_grid"] is not None:
        payload["gasolina_aditivada"] = payload["gasolina_grid"]
        payload.pop("gasolina_grid", None)

    return payload

# -------- Core --------
def enviar_precos_supabase(precos: dict) -> tuple[int, str]:
    """
    UPSERT na tabela 'precos_combustiveis'.
    on_conflict = (data_coleta, empresa)
    Retorna (status_code, texto)
    """
    load_dotenv()  # lê o .env da pasta atual
    SUPABASE_URL = _require_env("SUPABASE_URL").rstrip("/")
    SUPABASE_KEY = _require_env("SUPABASE_KEY")

    payload = _normalize_payload(precos)

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=data_coleta,empresa"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        # return=representation para você ver o JSON que ficou no banco
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    return resp.status_code, resp.text

def consultar_precos(empresa: str, limit: int = 5) -> tuple[int, str]:
    """
    Faz um GET nos últimos registros dessa empresa para conferência.
    """
    load_dotenv()
    SUPABASE_URL = _require_env("SUPABASE_URL").rstrip("/")
    SUPABASE_KEY = _require_env("SUPABASE_KEY")

    params = (
        f"empresa=eq.{requests.utils.quote(empresa)}"
        f"&order=data_coleta.desc"
        f"&limit={int(limit)}"
    )
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?{params}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    resp = requests.get(url, headers=headers, timeout=TIMEOUT)
    return resp.status_code, resp.text

# -------- Exemplo de uso --------
if __name__ == "__main__":
    # EXEMPLO: troque os valores abaixo pelo que seu robô coletou
    precos_do_dia = {
        "data_coleta": "2025-09-02",          # ou date.today()
        "empresa": "VIBRA MARQUES",
        "gasolina_comum": 5.4058,
        "gasolina_grid": 5.541,               # será mapeado para gasolina_aditivada
        "etanol_hidratado": 4.2442,
        "diesel_s10": 5.5501,
        "diesel_s10_aditivado": None          # preencha se existir esse preço
    }

    # 1) UPSERT
    status, body = enviar_precos_supabase(precos_do_dia)
    print("📡 UPSERT Status:", status)
    print("📄 UPSERT Resposta:", body if body else "(vazio)")

    # 2) GET de conferência
    status_get, body_get = consultar_precos(precos_do_dia["empresa"], limit=5)
    print("📡 GET Status:", status_get)
    print("📄 GET Resposta:", body_get if body_get else "(vazio)")