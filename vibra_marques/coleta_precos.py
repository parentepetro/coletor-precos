#!/usr/bin/env python3
"""
COLETOR AUTOMATIZADO COMPLETO
Versão 4.1 - Corrigido

Objetivo:
- Coletar preços das distribuidoras via Selenium
- Salvar no Supabase e backup local
"""

import os
import json
import time
import logging
import schedule
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, asdict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# SUPABASE
from supabase import create_client, Client

# CONFIGURAÇÃO DE LOG
logging.basicConfig(
    filename='log_coleta.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ========== CONFIGURAÇÕES SUPABASE ==========
SUPABASE_URL = "https://seu-projeto.supabase.co"
SUPABASE_KEY = "sua-api-key"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== MODELO DE DADO ==========
@dataclass
class PrecoCombustivel:
    distribuidora: str
    tipo: str
    preco: float
    data: str

# ========== CLASSE COLETOR ==========
class ColetorAutomatizadoCompleto:
    def __init__(self):
        self.driver = None
        self.precos: List[PrecoCombustivel] = []

    def setup_driver(self) -> bool:
        try:
            service = Service(ChromeDriverManager().install())
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            self.driver = webdriver.Chrome(service=service, options=options)
            return True
        except Exception as e:
            logging.error(f"Erro ao iniciar driver: {e}")
            return False

    def acessar_site_distribuidora(self, nome: str, url: str):
        try:
            self.driver.get(url)
            time.sleep(3)
            logging.info(f"Acessou {nome}")
        except Exception as e:
            logging.error(f"Erro ao acessar {nome}: {e}")

    def coletar_precos(self, distribuidora: str):
        try:
            # Exemplo: Buscando preços por regex ou XPath
            elementos = self.driver.find_elements(By.XPATH, "//div[contains(text(),'R$')]")
            for el in elementos:
                texto = el.text.strip().replace("R$", "").replace(",", ".")
                try:
                    preco = float(texto)
                    self.precos.append(PrecoCombustivel(
                        distribuidora=distribuidora,
                        tipo="gasolina_c",  # ajustar conforme a leitura
                        preco=preco,
                        data=datetime.now().strftime("%Y-%m-%d")
                    ))
                except ValueError:
                    continue
        except Exception as e:
            logging.error(f"Erro ao coletar preços de {distribuidora}: {e}")

    def salvar_supabase(self):
        try:
            for item in self.precos:
                supabase.table("precos_combustiveis").insert(asdict(item)).execute()
            logging.info("Preços enviados ao Supabase.")
        except Exception as e:
            logging.error(f"Erro ao salvar no Supabase: {e}")

    def salvar_backup_local(self):
        try:
            data = [asdict(p) for p in self.precos]
            with open("backup_precos.json", "w") as f:
                json.dump(data, f, indent=4)
            logging.info("Backup local salvo.")
        except Exception as e:
            logging.error(f"Erro ao salvar backup local: {e}")

    def executar_coleta_completa(self):
        if not self.setup_driver():
            logging.error("Driver não iniciado.")
            return

        try:
            # Exemplo de distribuidoras
            distribuidoras = {
                "Vibra": "https://distribuidora-vibra.com/login",
                "Ipiranga": "https://distribuidora-ipiranga.com/login"
            }

            for nome, url in distribuidoras.items():
                self.acessar_site_distribuidora(nome, url)
                self.coletar_precos(nome)

            self.salvar_supabase()
            self.salvar_backup_local()

        finally:
            if self.driver:
                self.driver.quit()

# ========== AGENDAMENTO ==========
def job():
    logging.info("Iniciando nova coleta")
    coletor = ColetorAutomatizadoCompleto()
    coletor.executar_coleta_completa()
    logging.info("Coleta finalizada")

schedule.every().day.at("08:00").do(job)

if __name__ == "__main__":
    logging.info("Script iniciado.")
    job()  # executa na hora
    while True:
        schedule.run_pending()
        time.sleep(60)