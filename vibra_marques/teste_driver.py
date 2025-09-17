from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Configuração do Chrome
options = Options()
options.add_argument("--start-maximized")
# Remova "--headless" para ver o navegador abrindo
# options.add_argument("--headless")

# Inicia o driver com o ChromeDriverManager
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Acessa o site
driver.get("https://cn.vibraenergia.com.br/login/")