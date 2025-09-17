import asyncio
from playwright.async_api import async_playwright
import json
import re
import requests
from datetime import date

# === CONFIGURAÇÕES ===
URL_LOGIN = "https://cn.vibraenergia.com.br/login/"
URL_VITRINE = "https://cn.vibraenergia.com.br/central-de-pedidos/#/vitrine"
USUARIO = "1116006"
SENHA = "gavilla2013"
PALAVRAS_CHAVE = ["GASOLINA", "ETANOL", "DIESEL", "ÓLEO", "COMBUSTÍVEL"]

# === SUPABASE ===
SUPABASE_URL = "https://axscepubiapspjbtmkbx.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF4c2NlcHViaWFwc3BqYnRta2J4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY2OTA5NDgsImV4cCI6MjA3MjI2Njk0OH0.noqwOdb-tgeJMVKCL18AkdHkWLJQl7eO0IqVLRfF8FA"  # service_role (NÃO use a anon)
TABELA = "precos_combustiveis"

# Cabeçalhos base
BASE_HEADERS = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json",
}

# === HELPERS SUPABASE ===
def _normalize_payload(d: dict) -> dict:
    """
    Mantém apenas colunas que existem na tabela e normaliza tipos.
    Sua tabela (pelos logs) tem: data_coleta, empresa, etanol_hidratado, gasolina_comum,
                                 gasolina_aditivada, diesel_s10, diesel_s500?, diesel_s10_aditivado?,
                                 created_at, updated_at
    """
    # mapeia/normaliza
    out = {
        "data_coleta": d.get("data_coleta"),
        "empresa": d.get("empresa"),
        "gasolina_comum": d.get("gasolina_comum"),
        "gasolina_aditivada": d.get("gasolina_aditivada"),  # já vem com esse nome no seu dicionário
        "etanol_hidratado": d.get("etanol_hidratado"),
        "diesel_s10": d.get("diesel_s10"),
        # opcionalmente inclua estes se realmente existem na tabela:
        # "diesel_s500": d.get("diesel_s500"),
        "diesel_s10_aditivado": d.get("diesel_s10_aditivado"),
    }

    # data_coleta -> YYYY-MM-DD (já está nesse formato)
    if isinstance(out["data_coleta"], date):
        out["data_coleta"] = out["data_coleta"].isoformat()

    # arredonda preços
    for k in ("gasolina_comum","gasolina_aditivada","etanol_hidratado","diesel_s10","diesel_s10_aditivado"):
        if out.get(k) is not None:
            try:
                out[k] = round(float(out[k]), 4)
            except Exception:
                out[k] = None

    return out

def supabase_upsert(dados: dict, return_representation: bool = True):
    """
    UPSERT por (data_coleta, empresa) com merge-duplicates.
    """
    payload = _normalize_payload(dados)
    url = f"{SUPABASE_URL}/rest/v1/{TABELA}?on_conflict=data_coleta,empresa"
    headers = dict(BASE_HEADERS)
    headers["Prefer"] = "resolution=merge-duplicates," + ("return=representation" if return_representation else "return=minimal")

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    return r.status_code, r.text

def supabase_get(empresa: str, limit: int = 5):
    from urllib.parse import quote
    url = f"{SUPABASE_URL}/rest/v1/{TABELA}?empresa=eq.{quote(empresa)}&order=data_coleta.desc&limit={limit}"
    r = requests.get(url, headers=BASE_HEADERS, timeout=30)
    return r.status_code, r.text

# === COLETA (Playwright assíncrono) ===
async def extrair_precos_vibra():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # visual para acompanhar; troque para True p/ ficar mais rápido
        context = await browser.new_context()
        page = await context.new_page()

        print("🌐 Acessando página de login...")
        await page.goto(URL_LOGIN, wait_until="domcontentloaded")
        # tenta seletor direto (ajuste conforme o portal)
        await page.fill('#usuario', USUARIO)
        await page.fill('#senha', SENHA)

        # clique no botão (ajuste se o id/texto for diferente)
        # tente múltiplos seletores para robustez
        clicked = False
        for sel in ['#btn-acessar','button[type="submit"]','button:has-text("Entrar")','button:has-text("Login")']:
            try:
                await page.click(sel, timeout=1500)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            # fallback: Enter no campo de senha
            await page.press('#senha', 'Enter')

        # navegação rápida para a vitrine
        await page.wait_for_timeout(1200)
        print("➡️ Indo para a vitrine...")
        await page.goto(URL_VITRINE, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)

        # scroll rápido (lazy loading dos cards)
        for _ in range(16):
            await page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight*0.95))")
            await page.wait_for_timeout(300)

        # mais um respiro curto
        await page.wait_for_timeout(800)

        print("📋 Coletando spans e preços visíveis...")
        elementos = await page.evaluate("""
            Array.from(document.querySelectorAll("span, strong")).map(el => ({
                tag: el.tagName,
                text: (el.textContent || "").trim()
            }));
        """)

        precos_extraidos = []
        ultimo_produto_valido = None

        for el in elementos:
            txt = (el["text"] or "").strip()
            if not txt:
                continue

            if el["tag"] == "SPAN":
                # considera produto quando bate com as palavras-chave
                if any(p in txt.upper() for p in PALAVRAS_CHAVE):
                    ultimo_produto_valido = txt

            elif el["tag"] == "STRONG" and ultimo_produto_valido:
                m = re.search(r"([\d.,]+)", txt)
                if m:
                    preco = float(m.group(1).replace(".", "").replace(",", ".")) if ("," in m.group(1)) else float(m.group(1))
                    # se for número do tipo 5.41 vs 5.411, arredonda 4 casas
                    preco = round(preco, 4)
                    precos_extraidos.append({
                        "produto": ultimo_produto_valido,
                        "valor": preco
                    })
                    ultimo_produto_valido = None

        # remover duplicatas (produto, valor)
        precos_unicos = []
        vistos = set()
        for item in precos_extraidos:
            chave = (item["produto"], item["valor"])
            if chave not in vistos:
                precos_unicos.append(item)
                vistos.add(chave)

        print(f"✅ {len(precos_unicos)} produtos válidos extraídos.")

        # monta dicionário final
        dados = {
            "data_coleta": date.today().isoformat(),
            "empresa": "VIBRA MARQUES"
        }

        for item in precos_unicos:
            nome = item["produto"].upper()
            preco = item["valor"]
            if "GASOLINA COMUM" in nome and "ADIT" not in nome:
                dados["gasolina_comum"] = preco
            elif "GASOLINA" in nome and "ADIT" in nome:
                dados["gasolina_aditivada"] = preco   # já no nome certo p/ tabela
            elif "ETANOL" in nome:
                dados["etanol_hidratado"] = preco
            elif "S10" in nome:
                dados["diesel_s10"] = preco
            elif "S500" in nome:
                # só inclua se sua tabela tiver a coluna; pelos logs estava null/ausente
                # dados["diesel_s500"] = preco
                pass
            elif "ÓLEO DIESEL" in nome:
                dados["diesel_s10_aditivado"] = preco

        print(f"🧾 Dados preparados: {dados}")

        # 💾 Backup local do payload final
        with open("precos_vibra.json", "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        print("📄 Arquivo salvo como 'precos_vibra.json'")

        # 📤 Envia (UPSERT) ao Supabase
        print("📤 Enviando (UPSERT) ao Supabase...")
        status, body = supabase_upsert(dados, return_representation=True)
        print("📡 UPSERT Status:", status)
        print("📄 UPSERT Resposta:", body if body else "(vazio)")

        # 🔎 Conferência (GET)
        print("🔎 Conferindo últimos registros...")
        s_get, b_get = supabase_get("VIBRA MARQUES", limit=5)
        print("📡 GET Status:", s_get)
        print("📄 GET Resposta:", b_get if b_get else "(vazio)")

        # Screenshot final (opcional)
        await page.screenshot(path="vitrine_final.png")
        print("🖼️ Screenshot salva (vitrine_final.png).")

        await browser.close()

# --- Executar ---
if __name__ == "__main__":
    asyncio.run(extrair_precos_vibra())