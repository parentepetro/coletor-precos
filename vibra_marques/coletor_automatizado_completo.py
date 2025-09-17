import asyncio
from playwright.async_api import async_playwright
import json
import re
import requests
from datetime import date
import os
from dotenv import load_dotenv

# === CARREGAR .ENV LOCAL ===
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

USUARIO = os.getenv("VIBRA_MARQUES_USER")
SENHA = os.getenv("VIBRA_MARQUES_PASS")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_KEY")
TABELA = os.getenv("TABELA")
EMPRESA = os.getenv("EMPRESA", "VIBRA MARQUES")

headers = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json",
}

URL_LOGIN = "https://cn.vibraenergia.com.br/login/"
URL_VITRINE = "https://cn.vibraenergia.com.br/central-de-pedidos/#/vitrine"

async def coletar_precos():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Login
        await page.goto(URL_LOGIN)
        await page.fill('input[name="cnpj"]', USUARIO)
        await page.fill('input[name="password"]', SENHA)
        await page.click('button[type="submit"]')

        await page.wait_for_url(URL_VITRINE, timeout=20000)
        await page.wait_for_timeout(5000)  # espera a vitrine carregar

        content = await page.content()
        pattern = r'([A-Z\s\d]+)\s+-\s+R\$\s+(\d+,\d+)'

        matches = re.findall(pattern, content)
        precos = {}
        for nome, valor in matches:
            nome = nome.upper().strip()
            valor_float = float(valor.replace(",", "."))

            if "GASOLINA COMUM" in nome:
                precos["gasolina_comum"] = valor_float
            elif "GASOLINA GRID" in nome or "GASOLINA ADITIVADA" in nome:
                precos["gasolina_aditivada"] = valor_float
            elif "ETANOL HIDRATADO" in nome:
                precos["etanol_hidratado"] = valor_float
            elif "DIESEL S10" in nome and "ADITIVADO" not in nome:
                precos["diesel_s10"] = valor_float
            elif "DIESEL S10 ADITIVADO" in nome:
                precos["diesel_s10_aditivado"] = valor_float

        precos["data_coleta"] = str(date.today())
        precos["empresa"] = EMPRESA

        print("✅ Dados preparados para envio:", precos)

        # Envio ao Supabase
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/{TABELA}",
            headers=headers,
            data=json.dumps(precos),
        )

        if response.status_code in [200, 201]:
            print("📤 Dados enviados com sucesso ao Supabase.")
        else:
            print("❌ Erro ao enviar para Supabase:", response.status_code, response.text)

        # Backup local
        with open("precos_vibra.json", "w") as f:
            json.dump(precos, f, indent=2)

        await page.screenshot(path="vitrine_final.png")
        print("🖼️ Screenshot salva como 'vitrine_final.png'")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(coletar_precos())