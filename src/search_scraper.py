import time
import random
import re
from typing import List, Optional, Dict, Any
from playwright.sync_api import sync_playwright, Page, BrowserContext
from src.database import DatabaseManager

class MercadoLivreSearch:
    """
    Scraper de busca do Mercado Livre.
    Utiliza a lógica original validada com seletores .poly-card e Regex específicos.
    """

    def __init__(self, db: DatabaseManager, headless: bool = False):
        self.db = db
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless, 
            slow_mo=200,
            args=["--disable-blink-features=AutomationControlled"]
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser: self.browser.close()
        if self.playwright: self.playwright.stop()

    def run(self, terms: List[str], pages_per_term: int = 3):
        """Método de entrada compatível com a chamada do main.py"""
        with self:
            self.processar_busca(terms, pages_per_term)

    @staticmethod
    def _extrair_id(link: str) -> Optional[str]:
        if not link: return None
        match = re.search(r'/p/(MLB\d+)', link)
        if match: return match.group(1)
        match = re.search(r'(MLB-?\d+)', link)
        if match: return match.group(1).replace('-', '')
        return None

    @staticmethod
    def _limpar_preco(texto: str) -> Optional[float]:
        if not texto: return None
        try:
            limpo = texto.replace('R$', '').replace('.', '').replace(',', '.').strip()
            return float(re.sub(r'[^\d\.]', '', limpo))
        except: return None

    @staticmethod
    def _extrair_vendidos(texto: str) -> int:
        if not texto: return 0
        try:
            texto_lower = texto.lower()
            multiplicador = 1000 if 'mil' in texto_lower else 1
            
            partes = texto_lower.rsplit('|', 1)
            if len(partes) == 1:
                qtd_texto =  partes[0].strip()
            else:
                qtd_texto = partes[1].strip()

            numeros_str = re.sub(r'[^\d\.,]', '', qtd_texto).replace(',', '.')
            if numeros_str:
                match = re.search(r'(\d+(\.\d+)?)', numeros_str)
                if match: return int(float(match.group(1)) * multiplicador)
            return 0
        except: return 0

    def processar_busca(self, termos: List[str], paginas_por_termo: int = 3):
        """
        Executa a busca iterando por termos e páginas.
        Salva os resultados diretamente no banco de dados.
        """
        for termo in termos:
            print(f"\n>>> Iniciando busca por: '{termo}'")
            # Cria contexto novo para cada termo (rotação de UA)
            context = self.browser.new_context(user_agent=random.choice(self.user_agents))
            page = context.new_page()
            
            termo_slug = termo.replace(" ", "-")
            ranking_global = 1

            for i in range(paginas_por_termo):
                offset = 1 + (i *48)
                if i == 0:
                    url = f"https://lista.mercadolivre.com.br/{termo_slug}_NoIndex_True"
                else:
                    url = f"https://lista.mercadolivre.com.br/{termo_slug}_Desde_{offset}_NoIndex_True"
                
                print(f"   -> Acessando página {i+1} (Offset {offset})...")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    time.sleep(random.uniform(2.0, 4.0))
                    
                    try: # Fecha cookies
                        page.click('button[data-testid="action:understood-button"]', timeout=1000)
                    except: pass

                    # Lógica original de seleção de cards
                    cards = page.query_selector_all('.poly-card') or page.query_selector_all('.ui-search-layout__item')
                    
                    if not cards: 
                        print("      Nenhum card encontrado. Parando paginação.")
                        break

                    print(f"      Encontrados {len(cards)} itens.")
                    
                    itens_salvos = 0
                    for card in cards:
                        # Extrai dados passando as variáveis de ranking e pagina
                        item = self._extrair_dados_card(card, ranking_global, i==0, termo)
                        if item:
                            # Salva no Banco de Dados
                            self.db.upsert_product_from_search(item)
                            ranking_global += 1
                            itens_salvos += 1
                    
                    print(f"      -> {itens_salvos} itens válidos processados.")

                except Exception as e:
                    print(f"      [Erro na página] {e}")
                    break
            
            context.close()
            time.sleep(random.uniform(3.0, 5.0))

    def _extrair_dados_card(self, card, ranking: int, is_first_page: bool, termo_busca: str) -> Optional[Dict[str, Any]]:
        """
        Extrai dados do card mantendo.
        """
        try:
            # Seletores originais
            title_el = card.query_selector('.poly-component__title a, .ui-search-item__title') or card.query_selector('a.poly-component__title')
            if not title_el: return None

            link = title_el.get_attribute('href')
            if 'click1' in link: return None
            id_mlb = self._extrair_id(link)
            
            if not id_mlb: return None # Proteção extra

            titulo = title_el.inner_text().strip()
            
            # Preços
            price_el = card.query_selector('.poly-price__current .andes-money-amount__fraction, .price-tag-amount .price-tag-fraction')
            preco_atual = self._limpar_preco(price_el.inner_text()) if price_el else 0.0

            orig_el = card.query_selector('.poly-component__price s .andes-money-amount__fraction, .ui-search-price__original-value .price-tag-fraction')
            preco_original = self._limpar_preco(orig_el.inner_text()) if orig_el else preco_atual

            # Flags e Textos
            txt = card.inner_text().lower()
            is_ad = 'patrocinado' in txt
            
            # Lógica de is_full
            is_full = True if card.query_selector('.poly-component__shipped-from svg use[href="#poly_full"], .ui-search-item__fulfillment-label') else False
            
            is_best_seller = 'mais vendido' in txt

            # Avaliação e Vendas
            avaliacao_nota = 0.0
            qtd_vendida = 0

            review_box = card.query_selector('.poly-component__review-compacted')
            if review_box:
                texto_box = review_box.inner_text()
                if 'vendido' in texto_box.lower():
                    qtd_vendida = self._extrair_vendidos(texto_box)
                
                span1 = review_box.query_selector('span')
                if span1 and 'vendido' not in span1.inner_text().lower():
                    # Tenta converter nota (ex: "4.5" -> 4.5)
                    try: avaliacao_nota = float(span1.inner_text().strip())
                    except: pass
            else:
                cond_el = card.query_selector('.poly-component__condition')
                if cond_el: qtd_vendida = self._extrair_vendidos(cond_el.inner_text())
                
                rate_el = card.query_selector('.poly-reviews__rating, .ui-search-reviews__rating-number')
                if rate_el: 
                    try: avaliacao_nota = float(rate_el.inner_text().strip())
                    except: pass

            # --- MAPEAMENTO PARA O BANCO DE DADOS ---
            return {
                "ml_id": id_mlb,                   
                "title": titulo,                   
                "permalink": link.split('#')[0].split('?')[0],
                "search_term": termo_busca,        
                "price_current": preco_atual,      
                "price_original": preco_original,  
                "is_ad": is_ad,
                "is_full": is_full,
                "is_best_seller": is_best_seller,  
                "sales_qty_search": qtd_vendida,   
                "reviews_rating_average": avaliacao_nota,
                "ranking_search": ranking,         
                "is_first_page": is_first_page     
            }
        except Exception as e:
            print(f"Erro ao extrair card: {e}") # Debug opcional
            return None