from playwright.sync_api import sync_playwright
import time
import json

def rolar_container(page, max_tentativas=20, intervalo=1.5):
    produtos_vistos = set()
    for tentativa in range(max_tentativas):
        cards = page.query_selector_all(".card-produto")
        novos = {card.inner_text() for card in cards}
        if novos <= produtos_vistos:
            break
        produtos_vistos |= novos

        # Rola o container com JavaScript
        page.evaluate("""
            const container = document.querySelector('.scrollbar-container');
            if (container) {
                container.scrollBy(0, container.scrollHeight);
            }
        """)
        time.sleep(intervalo)
    return list(produtos_vistos)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    print("🔐 Acessando login...")
    page.goto("https://cn.vibraenergia.com.br/login/", timeout=60000)

    page.fill("#usuario", "1116006")
    page.fill("#senha", "gavilla2013")
    page.click("#btn-acessar")

    print("⏳ Aguardando login...")
    page.wait_for_url("**/central-de-pedidos/**", timeout=60000)

    print("➡️ Redirecionando manualmente para a vitrine...")
    page.goto("https://cn.vibraenergia.com.br/central-de-pedidos/#/vitrine", timeout=60000)

    print("⏳ Aguardando vitrine carregar...")
    page.wait_for_selector(".card-produto", timeout=60000)

    print("🔄 Rolando vitrine para carregar todos os produtos...")
    produtos_raw = rolar_container(page)

    print(f"✅ {len(produtos_raw)} produtos extraídos!")

    produtos_estruturados = []
    for texto in produtos_raw:
        linhas = [l.strip() for l in texto.split("\n") if l.strip()]
        if len(linhas) >= 3:
            produto = {
                "nome": linhas[0],
                "codigo": next((l.split(":")[1].strip() for l in linhas if l.startswith("COD:")), None),
                "base": next((l.split(":")[1].strip() for l in linhas if l.startswith("Base:")), None),
                "preco": next((l.replace("R$", "").replace(",", ".").strip() for l in linhas if "R$" in l), None),
                "validade": next((l.strip() for l in linhas if "dia" in l.lower()), None)
            }
            produtos_estruturados.append(produto)

    with open("produtos_vitrine.json", "w", encoding="utf-8") as f:
        json.dump(produtos_estruturados, f, indent=2, ensure_ascii=False)

    print("📁 Arquivo salvo como 'produtos_vitrine.json'")
    browser.close()