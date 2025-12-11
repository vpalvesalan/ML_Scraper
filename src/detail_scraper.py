import re
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
from playwright.sync_api import sync_playwright, BrowserContext, Page
from src.database import DatabaseManager

# --- CONFIGURAÇÕES GERAIS (Mantidas do seu script) ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def parse_data_ptbr(texto_data: str) -> datetime:
    """Converte '08 abr. 2023' para datetime."""
    if not texto_data: return None
    texto = texto_data.lower().strip()
    meses = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
             'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
    try:
        texto_limpo = texto.replace('.', '').replace(' de ', ' ')
        partes = texto_limpo.split()
        if len(partes) >= 3:
            day = int(partes[0])
            month_str = partes[1][:3]
            year = int(partes[-1])
            month = meses.get(month_str, 1)
            return datetime(year, month, day)
        return None
    except: return None

def converter_vendas_ml(texto_vendas: str) -> int:
    """Converte '+5mil vendas' para inteiro."""
    if not texto_vendas: return 0
    texto = texto_vendas.lower().replace('+', '').replace('vendas', '').replace('vendidas', '').strip()
    try:
        if 'mil' in texto:
            numero = float(texto.replace('mil', '').replace(',', '.').strip())
            return int(numero * 1000)
        return int(texto.replace('.', ''))
    except: return 0

class MercadoLivreDetail:
    """
    Scraper de detalhes do produto.
    Lógica portada estritamente do script 'main.py' fornecido pelo usuário.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    def run(self, candidates: list):
        """
        Executa o loop de processamento para a lista de candidatos.
        """
        with sync_playwright() as p:
            print("🚀 Iniciando Motor do Navegador...")
            
            # Flags Anti-Detecção
            args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars"
            ]
            
            # Launch persistente
            browser = p.chromium.launch(headless=False, args=args)
            
            total = len(candidates)
            for i, item in enumerate(candidates):
                url = item['permalink']
                ml_id = item['ml_id']
                
                print(f"\n--- Processando {i+1}/{total}: {ml_id} ---")
                
                # --- CRIAÇÃO DE CONTEXTO (A cada produto) ---
                ua_atual = random.choice(USER_AGENTS)
                
                context = browser.new_context(
                    user_agent=ua_atual,
                    viewport={'width': 1366, 'height': 768},
                    locale='pt-BR'
                )
                
                # Injeta script para esconder webdriver
                context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)
                
                try:
                    # Chama a função de extração
                    product_payload, seller_payload = self._process_product(context, url)
                    
                    if product_payload:
                        # Salva no Banco de Dados
                        self.db.upsert_product_details(ml_id, product_payload, seller_payload)
                        print(f"   -> Sucesso! Baixados {product_payload['total_baixado']} comentários.")
                    
                    # Delay entre PRODUTOS
                    tempo_espera = random.uniform(3, 7)
                    print(f"💤 Aguardando {tempo_espera:.1f}s para o próximo...")
                    time.sleep(tempo_espera)
                    
                except Exception as e:
                    print(f"Erro genérico no loop: {e}")
                finally:
                    context.close()
            
            browser.close()
            print("\n✅ Processo de Detalhamento Finalizado!")

    def _process_product(self, context: BrowserContext, url_produto: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Função isolada que recebe um CONTEXTO já aberto e processa UM produto.
        Lógica idêntica ao script fornecido.
        """
        print(f"[*] Acessando: {url_produto}")
        page = context.new_page()
        
        # Estrutura de dados
        dados = {
            "url": url_produto,
            "titulo": None,
            "marca": None,
            "modelo": None,
            "num_avaliacoes": 0,
            "num_comentarios": 0,
            "num_comentarios_coletados": 0,
            "n_comentarios_ult_90_dias": 0,
            "data_ultimo_review": None,
            "dias_desde_ultimo_review": None,
            "mais_vendidos": 0,
            "resumo_ia": "Não disponível",
            "descricao": None,
            "categorias": {}, 
            "caracteristicas_completas": {},
            "dados_vendedor": {}, 

        }

        try:
            # Timeout maior para garantir carregamento
            page.goto(url_produto, timeout=80000)
            page.wait_for_load_state("domcontentloaded")
            
            # Delay aleatório inicial
            time.sleep(random.uniform(2, 4))

            # 1. Título
            try:
                h1 = page.query_selector('h1')
                if h1: dados['titulo'] = h1.inner_text()
            except: pass

            # 2. Mais vendido
            try:
                el_flag = page.query_selector('a[href*="mais-vendidos"]')
                dados['mais_vendido'] = 1 if el_flag else 0
            except: pass

            # 3. Categorias
            try:
                elementos_categoria = page.query_selector_all('ol.andes-breadcrumb li.andes-breadcrumb__item a')
                if elementos_categoria:
                    for i, el in enumerate(elementos_categoria, start=1):
                        dados['categorias'][f"categoria_{i}"] = el.inner_text().strip()
                else:
                    container_bread = page.query_selector('ol.andes-breadcrumb')
                    if container_bread:
                        partes = container_bread.inner_text().split('\n')
                        for i, parte in enumerate(partes, start=1):
                            if parte.strip(): dados['categorias'][f"categoria_{i}"] = parte.strip()
            except: pass

            # 4. Dados do Vendedor
            try:
                el_vendedor = page.query_selector('.ui-seller-data-header__title-container span')
                if not el_vendedor: el_vendedor = page.query_selector('.ui-seller-data-header__title-container h3')
                if el_vendedor: dados['dados_vendedor']['nome'] = el_vendedor.inner_text().strip()

                el = page.query_selector('.ui-seller-data-status__title')
                if el:
                    dados['dados_vendedor']['classificacao'] = el.inner_text().strip()
                else:
                    termo = page.query_selector('ul.ui-seller-data-status__thermometer')
                    v = termo.get_attribute('value') if termo else None
                    if v and v.isdigit():
                        dados['dados_vendedor']['classificacao'] = f"level {v}"
                    else:
                        dados['dados_vendedor']['classificacao'] = "Not Found"

                el_vendas = page.query_selector('.ui-seller-data-status__info-title')
                if el_vendas: dados['dados_vendedor']['vendas_total'] = converter_vendas_ml(el_vendas.inner_text())

                el_loja = page.query_selector('.ui-seller-data-header__subtitle-container')
                is_loja = False
                if el_loja:
                    if "loja oficial" in el_loja.inner_text().lower() or el_loja.query_selector('use[href="#verified_small"]'):
                        is_loja = True
                dados['dados_vendedor']['loja_oficial'] = is_loja
            except: pass

            # 5. Características
            try:
                rows = page.query_selector_all('tr.andes-table__row')
                for row in rows:
                    th = row.query_selector('th')
                    td = row.query_selector('td')
                    if th and td:
                        chave = th.inner_text().strip().replace(':', '')
                        valor = td.inner_text().strip()
                        dados['caracteristicas_completas'][chave] = valor
                        if 'Marca' in chave: dados['marca'] = valor
                        if 'Modelo' in chave: dados['modelo'] = valor
            except: pass

            # 6. Descrição e IA
            desc = page.query_selector('.ui-pdp-description__content')
            if desc: dados['descricao'] = desc.inner_text()
            ia_summary = page.query_selector('.ui-review-capability__summary__plain_text__summary_container')
            if ia_summary: dados['resumo_ia'] = ia_summary.inner_text()

            # 7. Avaliações (Count e Num)
            try:
                lbl_avaliacoes = page.query_selector('p.ui-review-capability__rating__label')
                if lbl_avaliacoes:
                    nums = re.findall(r'\d+', lbl_avaliacoes.inner_text().replace('.', ''))
                    if nums: dados['num_avaliacoes'] = int(nums[0])
                
                span_op = page.query_selector('span.total-opinion')
                if span_op:
                    nums = re.findall(r'\d+', span_op.inner_text())
                    if nums: dados['num_comentarios'] = int(nums[0])
            except: pass

            # 8. Reviews
            print("      Buscando reviews...")
            page.evaluate("window.scrollBy(0, 500)")
            
            btn_comentarios = page.query_selector('button[data-testid="see-more"]')
            if not btn_comentarios: btn_comentarios = page.query_selector('button.show-more-click')
            if not btn_comentarios: btn_comentarios = page.query_selector('a.ui-pdp-reviews__see-more')

            if btn_comentarios:
                try:
                    time.sleep(random.uniform(1, 2))
                    btn_comentarios.scroll_into_view_if_needed()
                    btn_comentarios.click()
                    
                    elemento_iframe = page.wait_for_selector('iframe#ui-pdp-iframe-reviews', state="attached", timeout=15000)
                    frame_reviews = elemento_iframe.content_frame()
                    
                    if frame_reviews:
                        frame_reviews.wait_for_load_state("domcontentloaded")
                        time.sleep(2)
                        
                        last_height = frame_reviews.evaluate("document.body.scrollHeight")
                        scrolls = 0
                        max_scrolls = 150
                        
                        while scrolls < max_scrolls:
                            frame_reviews.evaluate("window.scrollBy(0, 1000)")
                            time.sleep(random.uniform(0.5, 0.9))
                            
                            current_scroll = frame_reviews.evaluate("window.scrollY + window.innerHeight")
                            new_height = frame_reviews.evaluate("document.body.scrollHeight")
                            
                            if current_scroll >= new_height:
                                time.sleep(1.5)
                                new_height = frame_reviews.evaluate("document.body.scrollHeight")
                                if frame_reviews.evaluate("window.scrollY + window.innerHeight") >= new_height:
                                    break
                            scrolls += 1

                        selectors_data = [
                            'span.ui-review-capability-comments__comment__date',
                            'p.ui-review-capability-comments__comment__date',
                            'span.ui-review-card__metadata__date', 'time'
                        ]
                        
                        datas_encontradas = []
                        for sel in selectors_data:
                            els = frame_reviews.query_selector_all(sel)
                            if els:
                                for el in els:
                                    txt = el.inner_text()
                                    dt = parse_data_ptbr(txt)
                                    if dt: datas_encontradas.append(dt)
                                if datas_encontradas: break
                        
                        if datas_encontradas:
                            dados['num_comentarios_coletados'] = len(datas_encontradas)
                            ultima_data = max(datas_encontradas)
                            dados['data_ultimo_review'] = ultima_data.strftime('%Y-%m-%d') # Ajustado para salvar no DB (ISO 8601)
                            dados['dias_desde_ultimo_review'] = (datetime.now() - ultima_data).days
                            
                            limite = datetime.now() - timedelta(days=90)
                            recentes = [d for d in datas_encontradas if d >= limite]
                            dados['n_comentarios_ult_90_dias'] = len(recentes)
                            
                            print(f"      [OK] {len(datas_encontradas)} reviews coletados ({len(recentes)} recentes).")
                        else:
                            print("      [AVISO] Iframe aberto mas sem datas detectadas.")
                except Exception as e:
                    print(f"      [ERRO] Falha ao processar reviews: {e}")

            if not btn_comentarios:
                print("      [INFO] Botão de reviews não encontrado. Coletando direto da página!")

                seletores_inline = [
                    'article.ui-review-capability-comments__comment span.ui-review-capability-comments__comment__date',
                    'span.ui-review-capability-comments__comment__date',
                    'p.ui-review-capability-comments__comment__date',
                    'time'
                ]
                
                datas_inline = []

                for sel in seletores_inline:
                    els = page.query_selector_all(sel)
                    if els:
                        for el in els:
                            txt = el.inner_text()
                            dt = parse_data_ptbr(txt)
                            if dt:
                                datas_inline.append(dt)
                        if datas_inline:
                            break

                if datas_inline:
                    dados['num_comentarios_coletados'] = len(datas_inline)
                    ultima_data = max(datas_inline)
                    dados['data_ultimo_review'] = ultima_data.strftime('%Y-%m-%d') # Ajustado para salvar no DB
                    dados['dias_desde_ultimo_review'] = (datetime.now() - ultima_data).days
                    
                    limite = datetime.now() - timedelta(days=90)
                    recentes = [d for d in datas_inline if d >= limite]
                    dados['n_comentarios_ult_90_dias'] = len(recentes)

                    print(f"      [OK] {len(datas_inline)} reviews coletados direto da página ({len(recentes)} recentes).")
                else:
                    print("      [AVISO] Nenhuma data encontrada no modo inline.")

            # --- ADAPTAÇÃO PARA RETORNO DO BANCO DE DADOS ---
            seller_payload = dados.get('dados_vendedor', {})
            
            # Mapeamos as chaves para as chaves esperadas pelo DatabaseManager.upsert_product_details
            product_payload = {
                'marca': dados.get('marca'),
                'modelo': dados.get('modelo'),
                'caracteristicas_completas': dados.get('caracteristicas_completas'),
                'categorias': dados.get('categorias'),
                
                'num_avaliacoes': dados.get('num_avaliacoes'),
                'data_ultimo_review': dados.get('data_ultimo_review'),
                'dias_desde_ultimo_review': dados.get('dias_desde_ultimo_review'),
                'mais_vendido': dados.get('mais_vendido'),
                'resumo_ia': dados.get('resumo_ia'),
                'descricao': dados.get('descricao'),
                
                'total_disponivel': dados.get('num_comentarios'),      
                'total_baixado': dados.get('num_comentarios_coletados'),
                'ultimos_90d': dados.get('n_comentarios_ult_90_dias') 
            }

            return product_payload, seller_payload

        except Exception as e:
            print(f"   [CRÍTICO] Erro na página: {e}")
            return None, None
        
        finally:
            page.close()