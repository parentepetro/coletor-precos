
import asyncio
from playwright.async_api import async_playwright
import json
import re
import requests
from datetime import date

# === VARIÁVEIS DE AMBIENTE ===
from dotenv import load_dotenv
import os

# Carrega o arquivo .env
load_dotenv(dotenv_path="/Users/guiparente/robo_precos/.env")  # ajuste se mudar o caminho

# Pega as variáveis do .env
USUARIO = os.getenv("VIBRA_MARQUES_USER")
SENHA = os.getenv("VIBRA_MARQUES_PASS")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_KEY")
TABELA = "precos_combustiveis"

# Palavras-chave para identificar produtos
PALAVRAS_CHAVE = ["GASOLINA", "ETANOL", "DIESEL", "ÓLEO", "COMBUSTÍVEL"]

# Headers para envio ao Supabase
headers = {
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Content-Type": "application/json"
}


async def extrair_precos_vibra():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # altere para True se quiser rodar oculto
        context = await browser.new_context()
        page = await context.new_page()

        # Acessa login
        print("🌐 Acessando página de login...")
        await page.goto("https://cn.vibraenergia.com.br/login/", wait_until="networkidle")
        await page.wait_for_selector('#usuario', timeout=10000)
        await page.fill('#usuario', USUARIO)
        await page.fill('#senha', SENHA)
        await page.click('#btn-acessar')

        print("⏳ Aguardando redirecionamento...")
        await page.wait_for_timeout(5000)
        await page.goto("https://cn.vibraenergia.com.br/central-de-pedidos/#/vitrine", wait_until="networkidle")
        await page.wait_for_timeout(5000)

        print("🔄 Rolando para baixo...")
        for _ in range(25):
            await page.evaluate("window.scrollBy(0, 1000)")
            await page.wait_for_timeout(500)

        print("📋 Coletando spans e preços visíveis...")
        elementos = await page.evaluate("""
            Array.from(document.querySelectorAll("span.item-descricao, strong")).map(el => ({
                tag: el.tagName,
                text: el.textContent.trim()
            }));
        """)

        precos_extraidos = []
        ultimo_produto_valido = None

        for el in elementos:
            if el["tag"] == "SPAN":
                texto = el["text"].strip()
                if any(p in texto.upper() for p in PALAVRAS_CHAVE):
                    ultimo_produto_valido = texto
            elif el["tag"] == "STRONG" and ultimo_produto_valido:
                match = re.search(r"([\d.,]+)", el["text"])
                if match:
                    preco = float(match.group(1).replace(",", "."))
                    precos_extraidos.append({
                        "produto": ultimo_produto_valido,
                        "valor": preco
                    })
                    ultimo_produto_valido = None

        # Remover duplicatas
        precos_unicos = []
        vistos = set()
        for item in precos_extraidos:
            chave = (item["produto"], item["valor"])
            if chave not in vistos:
                precos_unicos.append(item)
                vistos.add(chave)

        print(f"✅ {len(precos_unicos)} produtos válidos extraídos.")

        # Monta dicionário final
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
                dados["gasolina_aditivada"] = preco
            elif "ETANOL" in nome:
                dados["etanol_hidratado"] = preco
            elif "S10" in nome:
                dados["diesel_s10"] = preco
            elif "S500" in nome:
                continue
            elif "ÓLEO DIESEL" in nome:
                dados["diesel_s10_aditivado"] = preco

        print(f"📦 Dados prontos para envio: {dados}")

        # Envia ao Supabase
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/{TABELA}",
            headers=headers,
            json=[dados]
        )

        if response.status_code in [200, 201]:
            print("📤 Dados enviados com sucesso ao Supabase.")
        else:
            print(f"❌ Erro ao enviar para Supabase: {response.status_code} {response.text}")

        # Screenshot de backup visual
        await page.screenshot(path="vitrine_final.png")
        print("🖼️ Screenshot salva como 'vitrine_final.png'")

        # Backup local JSON
        with open("precos_vibra.json", "w", encoding="utf-8") as f:
            json.dump(precos_unicos, f, ensure_ascii=False, indent=2)
        print("📄 Backup salvo como 'precos_vibra.json'")

        await browser.close()


# Executa o script
if __name__ == "__main__":
    asyncio.run(extrair_precos_vibra())
