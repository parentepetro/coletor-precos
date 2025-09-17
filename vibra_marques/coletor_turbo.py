# coletor_turbo.py
from dotenv import load_dotenv
from pathlib import Path
from datetime import date
from typing import Dict, Any, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
import os, re, requests, time, logging

# ------------ Configs rápidas ------------
DOTENV_PATH = Path(__file__).with_name(".env")
URL_LOGIN = "https://cn.vibraenergia.com.br/login/"
URL_VITRINE = "https://cn.vibraenergia.com.br/central-de-pedidos/#/vitrine"
EMPRESA = "VIBRA MARQUES"
TABLE = "precos_combustiveis"
HTTP_TIMEOUT = 20
RETRIES_HTTP = 2
PW_TIMEOUT = 45000  # 45s

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("turbo")

# ------------ Util ------------
def env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Variável de ambiente ausente: {name}")
    return val

def to_bool(v: str) -> bool:
    return str(v).strip().lower() in ("1","true","yes","y","on")

NUM_RE = re.compile(r"([-+]?\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{2,4})|[-+]?\d+[.,]\d{2,4})")
def parse_price(s: str) -> Optional[float]:
    m = NUM_RE.search(s)
    if not m:
        return None
    x = m.group(1).replace(" ", "")
    if "," in x and "." in x:
        x = x.replace(".", "")
    x = x.replace(",", ".")
    try:
        return round(float(x), 4)
    except ValueError:
        return None

def normalize_payload(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p)
    if isinstance(out.get("data_coleta"), date):
        out["data_coleta"] = out["data_coleta"].isoformat()
    if "gasolina_grid" in out and out["gasolina_grid"] is not None:
        out["gasolina_aditivada"] = out["gasolina_grid"]
        out.pop("gasolina_grid", None)
    for k in ("gasolina_comum","gasolina_aditivada","etanol_hidratado","diesel_s10","diesel_s10_aditivado"):
        if k in out and out[k] is not None:
            out[k] = round(float(out[k]), 4)
    return out

# ------------ Supabase ------------
class SB:
    def __init__(self):
        load_dotenv(dotenv_path=DOTENV_PATH)
        self.base = env("SUPABASE_URL").rstrip("/")
        self.key = env("SUPABASE_KEY")

    def upsert(self, precos: Dict[str, Any], return_representation: bool=False) -> Tuple[int, str]:
        url = f"{self.base}/rest/v1/{TABLE}?on_conflict=data_coleta,empresa"
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates," + ("return=representation" if return_representation else "return=minimal"),
        }
        body = normalize_payload(precos)
        for i in range(RETRIES_HTTP):
            try:
                r = requests.post(url, headers=headers, json=body, timeout=HTTP_TIMEOUT)
                if r.status_code >= 500 and i < RETRIES_HTTP-1:
                    time.sleep(1.2*(i+1))
                    continue
                return r.status_code, r.text or ""
            except requests.RequestException as e:
                if i == RETRIES_HTTP-1:
                    return 0, f"Erro de rede: {e}"
                time.sleep(1.2*(i+1))
        return 0, "Erro desconhecido"

# ------------ Coleta Playwright enxuta ------------
def _try_fill_credentials(page, user: str, pwd: str) -> bool:
    candidates = [
        ('input[name="username"]', 'input[name="password"]'),
        ('input[name="usuario"]',  'input[name="senha"]'),
        ('input[formcontrolname="username"]','input[formcontrolname="password"]'),
        ('input[placeholder*="Usu" i]','input[placeholder*="Sen" i]'),
        ('input[type="text"]','input[type="password"]'),
        ('#username','#password'),
    ]
    for su, sp in candidates:
        try:
            page.locator(su).first.fill(user, timeout=2500)
            page.locator(sp).first.fill(pwd,  timeout=2500)
            return True
        except Exception:
            continue
    return False

def coletar() -> Dict[str, Any]:
    load_dotenv(dotenv_path=DOTENV_PATH)
    user = env("VIBRA_MARQUES_USER")
    pwd  = env("VIBRA_MARQUES_PASS")
    headless = to_bool(os.getenv("HEADLESS","true"))

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, args=["--disable-gpu"])
        ctx = browser.new_context(ignore_https_errors=True, viewport={"width": 1280, "height": 800})
        ctx.set_default_timeout(PW_TIMEOUT)
        page = ctx.new_page()
        try:
            # Login rápido
            page.goto(URL_LOGIN, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            filled = _try_fill_credentials(page, user, pwd)
            if not filled:
                # checa iframes
                for fr in page.frames:
                    if fr == page.main_frame:
                        continue
                    try:
                        if fr.locator('input[type="password"]').first.count() > 0 and _try_fill_credentials(fr, user, pwd):
                            page = fr
                            break
                    except Exception:
                        continue
            if not filled and page == ctx.pages[0]:
                raise RuntimeError("Campos de login não encontrados.")

            # submit
            clicked = False
            for b in ('button[type="submit"]','button:has-text("Entrar")','button:has-text("Login")','input[type="submit"]','[role="button"]'):
                try:
                    page.locator(b).first.click(timeout=2500)
                    clicked = True
                    break
                except Exception:
                    continue
            if not clicked:
                try: page.locator('input[type="password"]').first.press("Enter")
                except Exception: pass
            try:
                ctx.pages[0].wait_for_load_state("networkidle", timeout=PW_TIMEOUT)
            except Exception:
                pass

            # Vitrine
            root = ctx.pages[0]
            root.goto(URL_VITRINE, wait_until="domcontentloaded")
            root.wait_for_load_state("networkidle")
            body_text = root.locator("body").inner_text(timeout=15000)
        finally:
            ctx.close()
            browser.close()

    # Parsing minimal
    t = body_text.lower()
    CHAVES = {
        "gasolina_comum":   [r"gasolina\s*comum", r"\bgc\b", r"gasolina\s*comum\s*claro"],
        "gasolina_grid":    [r"gasolina\s*(grid|aditivada)", r"\bga\b", r"aditivad"],
        "etanol_hidratado": [r"etanol\s*hidratado", r"etanol\s*hid", r"\beh\b"],
        "diesel_s10":       [r"diesel\s*s[-\s]*10", r"\bs[-\s]*10\b"],
    }
    precos = {k: None for k in ["gasolina_comum","gasolina_grid","etanol_hidratado","diesel_s10","diesel_s10_aditivado"]}
    for campo, pads in CHAVES.items():
        val = None
        for pat in pads:
            m = re.search(pat + r".{0,80}?" + NUM_RE.pattern, t, flags=re.IGNORECASE|re.DOTALL)
            if m:
                val = parse_price(m.group(0))
                if val is not None:
                    break
        precos[campo] = val

    return {
        "data_coleta": date.today(),
        "empresa": EMPRESA,
        "gasolina_comum": precos["gasolina_comum"],
        "gasolina_grid": precos["gasolina_grid"],
        "etanol_hidratado": precos["etanol_hidratado"],
        "diesel_s10": precos["diesel_s10"],
        "diesel_s10_aditivado": None,  # preencha se o site mostrar
    }

# ------------ MAIN ------------
def main():
    try:
        payload = coletar()
        log.info("Coleta: %s", payload)

        sb = SB()
        # usar retorno minimal (mais rápido)
        status, body = sb.upsert(payload, return_representation=False)
        print("📡 UPSERT Status:", status)
        print("📄 UPSERT Resposta:", body if body else "(vazio)")
    except Exception as e:
        log.exception("Falha no coletor turbo: %s", e)
        print("❌ Erro:", e)

if __name__ == "__main__":
    main()