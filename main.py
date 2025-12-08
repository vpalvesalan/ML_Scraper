import argparse
import sys
from src.database import DatabaseManager
from src.search_scraper import MercadoLivreSearch 
from src.detail_scraper import MercadoLivreDetail

def configurar_parser():
    """Configura os argumentos aceitos pela linha de comando."""
    parser = argparse.ArgumentParser(description="Sistema de Inteligência de Mercado - Mercado Livre")
    
    # Modo de operação
    parser.add_argument(
        '--mode', 
        choices=['search', 'detail', 'full'], 
        default='full',
        help="Modo de execução: 'search' (apenas busca), 'detail' (apenas detalhes) ou 'full' (ambos)."
    )

    # Argumentos para Busca
    parser.add_argument(
        '--terms', 
        nargs='+', 
        help="Lista de termos para busca (ex: 'lustre sindora' 'pendente'). Obrigatório para modos 'search' e 'full'."
    )
    
    parser.add_argument(
        '--pages', 
        type=int, 
        default=3, 
        help="Número de páginas a percorrer por termo (padrão: 3)."
    )

    # Argumentos para Detalhes (Filtros)
    parser.add_argument(
        '--min-price', 
        type=float, 
        default=0.0, 
        help="Preço mínimo para selecionar itens para enriquecimento."
    )
    
    parser.add_argument(
        '--min-rating', 
        type=float, 
        default=0.0, 
        help="Nota mínima (0 a 5) para selecionar itens."
    )
    
    parser.add_argument(
        '--limit', 
        type=int, 
        default=20, 
        help="Limite de produtos a serem detalhados nesta execução."
    )

    parser.add_argument(
        '--search-term', 
        type=str, 
        default=None, 
        help="Filtra candidatos para enriquecer apenas deste termo de busca (ex: 'lustre sindora')."
    )

    parser.add_argument(
        '--days-since-update', 
        type=int, 
        default=0, 
        help="Seleciona apenas itens não atualizados há X dias (ex: 30). Padrão: 0 (todos)."
    )

    parser.add_argument(
        '--only-new', 
        action='store_true', 
        help="Se usado, busca APENAS itens novos que nunca foram detalhados (ignora atualizações de antigos)."
    )

    return parser

def executar_busca(db, termos, paginas):
    if not termos:
        print("[ERRO] Para executar a busca, você deve fornecer termos usando --terms")
        sys.exit(1)

    print(f"\n[MODO BUSCA] Iniciando varredura para: {termos}")
    search_bot = MercadoLivreSearch(db)
    search_bot.run(termos, pages_per_term=paginas)

def executar_enriquecimento(db, min_price, min_rating, days_since_update, search_term, only_new, limit):
    
    msg_termo = f", Termo='{search_term}'" if search_term else ""
    print(f"\n[MODO DETALHE] Buscando candidatos (Preço > {min_price}, Nota > {min_rating}, Dias > {days_since_update}{msg_termo})...")
    
    candidatos = db.get_candidates_for_enrichment(
        min_price=min_price,
        min_rating=min_rating,
        days_since_update=days_since_update,
        search_term=search_term,
        only_new=only_new,  
        limit=limit
    )
    
    print(f"-> {len(candidatos)} produtos encontrados no banco pendentes de detalhes/atualização.")
    
    if candidatos:
        detail_bot = MercadoLivreDetail(db)
        detail_bot.run(candidatos)
    else:
        print("-> Nenhum candidato encontrado com esses filtros. Tente rodar a busca novamente ou baixar os critérios.")
        return

def main():
    parser = configurar_parser()
    args = parser.parse_args()
    
    print("=== INICIANDO SISTEMA DE INTELIGÊNCIA DE MERCADO ===")
    
    # Inicializa Banco (caminho relativo assumindo execução na raiz)
    db = DatabaseManager("ml_intelligence.db")
    
    # 1. Executa Busca (Se mode for 'search' ou 'full')
    if args.mode in ['search', 'full']:
        executar_busca(db, args.terms, args.pages)
    
    # 2. Executa Detalhes (Se mode for 'detail' ou 'full')
    if args.mode in ['detail', 'full']:
        executar_enriquecimento(
            db, 
            args.min_price, 
            args.min_rating, 
            args.days_since_update,
            args.search_term,
            args.only_new,
            args.limit
        )

    print("\n=== PROCESSO FINALIZADO ===")

if __name__ == "__main__":
    main()