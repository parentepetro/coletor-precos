# supabase_insert.py
from dotenv import load_dotenv
import os
import requests
from datetime import date

# 1) Carregar variáveis do .env
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL ou SUPABASE_KEY não encontrados no .env")

TABLE = "precos_combustiveis"

def enviar_precos_supabase(precos: dict) -> tuple[int, str]:
    """
    Envia preços para a tabela 'precos_combustiveis' com UPSERT.
    - Converte gasolina_grid -> gasolina_aditivada
    - Aceita diesel_s10_aditivado (opcional)
    precos esperado (exemplo):
    {
        "data_coleta": "2025-09-02",     # str (YYYY-MM-DD) ou date
        "empresa": "VIBRA MARQUES",
        "gasolina_comum": 5.4058,
        "gasolina_aditivada": 5.541,     # será sobrescrito se houver gasolina_grid
        "gasolina_grid": 5.541,          # opcional; se vier, vira gasolina_aditivada
        "etanol_hidratado": 4.2442,
        "diesel_s10": 5.5501,
        "diesel_s10_aditivado": None     # opcional
    }
    """
    # Normalizações
    payload = dict(precos)

    # data_coleta como string YYYY-MM-DD
    if isinstance(payload.get("data_coleta"), date):
        payload["data_coleta"] = payload["data_coleta"].isoformat()

    # Regra: gasolina_grid -> gasolina_aditivada
    if "gasolina_grid" in payload and payload["gasolina_grid"] is not None:
        payload["gasolina_aditivada"] = payload["gasolina_grid"]
        payload.pop("gasolina_grid", None)

    # Monta requisição com UPSERT:
    # - on_conflict=data_coleta,empresa (chave única)
    # - Prefer: resolution=merge-duplicates (mescla/atualiza)
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=data_coleta,empresa"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    return r.status_code, r.text

if __name__ == "__main__":
    # Exemplo de uso:
    precos_do_dia = {
        "data_coleta": "2025-09-02",
        "empresa": "VIBRA MARQUES",
        "gasolina_comum": 5.4058,
        "gasolina_grid": 5.541,          # vira gasolina_aditivada
        "etanol_hidratado": 4.2442,
        "diesel_s10": 5.5501,
        "diesel_s10_aditivado": None     # preencha se tiver
    }

    status, resp = enviar_precos_supabase(precos_do_dia)
    print("📡 Status:", status)
    print("📄 Resposta:", resp if resp else "(vazio)")