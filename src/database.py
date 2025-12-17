import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any

class DatabaseManager:
    """
    Gerencia a persistência de dados em SQLite.
    """

    def __init__(self, db_name: str = "ml_intelligence.db"):
        os.makedirs("data", exist_ok=True)
        self.db_path = os.path.join("data", db_name)
        self._setup_tables()

    def _get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _setup_tables(self):
        """
        Cria as tabelas necessárias.
        ATENÇÃO: Apague o arquivo .db antigo para que as novas colunas sejam criadas!
        """
        sql_products = """
        CREATE TABLE IF NOT EXISTS products (
            ml_id TEXT PRIMARY KEY,
            title TEXT,
            permalink TEXT,
            search_term TEXT,
            link_term TEXT,
            
            -- Dados de Busca
            price_current REAL,
            price_original REAL,
            is_best_seller INTEGER,
            is_full INTEGER,
            is_ad INTEGER,
            sales_qty_search INTEGER,
            reviews_rating_average REAL,
            is_international INTEGER,
            ranking_search INTEGER,
            is_first_page INTEGER,
            
            -- Dados de Detalhe (Enriched)
            brand TEXT,
            model TEXT,
            specifications_json TEXT,
            categories_json TEXT,
            reviews_rating_count INTEGER,
            last_comment_date TEXT,
            days_since_last_comment INTEGER,
            ai_summary TEXT,
            immediate_availability INTEGER,         
            description TEXT,              
            comments_total_available INTEGER,
            comments_fetched_count INTEGER,
            comments_last_90d INTEGER,
            
            seller_name TEXT,
            status TEXT DEFAULT 'DISCOVERED',
            last_updated DATETIME
        );
        """
        
        sql_sellers = """
        CREATE TABLE IF NOT EXISTS sellers (
            seller_name TEXT PRIMARY KEY,
            is_official_store INTEGER,
            sales_level TEXT,
            total_sales_history INTEGER,
            last_updated DATETIME
        );
        """

        with self._get_connection() as conn:
            conn.execute(sql_products)
            conn.execute(sql_sellers)

    def upsert_product_from_search(self, item: Dict[str, Any]):
        # (Este método permanece INALTERADO, mantendo a lógica aprovada anteriormente)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = """
        INSERT INTO products (
            ml_id, title, permalink, search_term, link_term,
            price_current, price_original,
            is_best_seller, is_full, is_ad, 
            sales_qty_search, reviews_rating_average, is_international,
            ranking_search, is_first_page,
            status, last_updated
        ) VALUES (
            :ml_id, :title, :permalink, :search_term, :link_term,
            :price_current, :price_original,
            :is_best_seller, :is_full, :is_ad, 
            :sales_qty_search, :reviews_rating_average, :is_international,
            :ranking_search, :is_first_page,
            'DISCOVERED', :last_updated
        )
        ON CONFLICT(ml_id) DO UPDATE SET
            price_current=excluded.price_current,
            price_original=excluded.price_original,
            sales_qty_search=excluded.sales_qty_search,
            search_term=excluded.search_term,
            link_term=excluded.link_term,
            is_ad=excluded.is_ad,
            ranking_search=excluded.ranking_search,
            is_first_page=excluded.is_first_page,
            last_updated=excluded.last_updated;
        """
        item['last_updated'] = now
        for f in ['is_best_seller', 'is_full', 'is_ad', 'is_first_page', 'is_international', 'immediate_availability']:
            item[f] = 1 if item.get(f) else 0
        with self._get_connection() as conn:
            conn.execute(sql, item)

    def upsert_product_details(self, ml_id: str, details: Dict[str, Any], seller_data: Dict[str, Any]):
        """
        Atualiza o produto com os dados ricos, incluindo as 5 novas variáveis.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1. Upsert Vendedor (Mantido)
        if seller_data and seller_data.get('nome'):
            sql_seller = """
            INSERT INTO sellers (seller_name, is_official_store, sales_level, total_sales_history, last_updated)
            VALUES (:nome, :loja_oficial, :classificacao, :vendas_total, :last_updated)
            ON CONFLICT(seller_name) DO UPDATE SET total_sales_history=excluded.total_sales_history, last_updated=excluded.last_updated;
            """
            s_data = seller_data.copy()
            s_data['last_updated'] = now
            s_data['loja_oficial'] = 1 if s_data.get('loja_oficial') else 0
            with self._get_connection() as conn: conn.execute(sql_seller, s_data)

        # 2. Update Produto
        sql_product = """
        UPDATE products SET
            brand = ?,
            model = ?,
            specifications_json = ?,
            categories_json = ?,
            
            reviews_rating_count = ?,   
            last_comment_date = ?,       
            days_since_last_comment = ?,
            is_best_seller = ?, 
            ai_summary = ?,             
            description = ?,
            is_international=?,
            immediate_availability=?, 
            
            comments_total_available = ?,
            comments_fetched_count = ?,
            comments_last_90d = ?,
            seller_name = ?,
            status = 'ENRICHED',
            last_updated = ?
        WHERE ml_id = ?
        """
        
        params = (
            details.get('marca'),
            details.get('modelo'),
            json.dumps(details.get('caracteristicas_completas', {})),
            json.dumps(details.get('categorias', {})),
            
            details.get('num_avaliacoes', 0),       
            details.get('data_ultimo_review'),       
            details.get('dias_desde_ultimo_review'),
            details.get('mais_vendido'),
            details.get('resumo_ia'),               
            details.get('descricao'),
            details.get('compra_internacional'),
            details.get('tempo_disponibilidade'),
            
            details.get('total_disponivel', 0),
            details.get('total_baixado', 0),
            details.get('ultimos_90d', 0),
            seller_data.get('nome') if seller_data else None,
            now,
            ml_id
        )

        with self._get_connection() as conn:
            conn.execute(sql_product, params)

    def get_candidates_for_enrichment(self, min_price=0, min_rating=0, min_sales=0,  days_since_update=0, search_term=None, only_new=False, limit=50):

        cutoff_str = (datetime.now() - timedelta(days=days_since_update)).strftime("%Y-%m-%d %H:%M:%S")

        if only_new:
            base_query = "WHERE status = 'DISCOVERED'"
        else:
            base_query = "WHERE (status = 'DISCOVERED' OR status = 'ENRICHED')"

        query_parts = ["SELECT ml_id, permalink, title, last_updated", "FROM products", base_query]

        params = []
        if search_term:
            query_parts.append("AND search_term = ?")
            params.append(search_term)

        query_parts.append("AND price_current >= ?")
        params.append(min_price)

        query_parts.append("AND (reviews_rating_average >= ? OR reviews_rating_average IS NULL)")
        params.append(min_rating)

        query_parts.append("AND sales_qty_search >= ?")
        params.append(min_sales)

        query_parts.append("AND last_updated <= ?")
        params.append(cutoff_str)

        # query_parts.append("ORDER BY last_updated DESC LIMIT ?")
        query_parts.append("ORDER BY RANDOM() LIMIT ?")
        params.append(limit)

        sql = "\n".join(query_parts)

        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        

# Obtém dados do banco de dados


def carregar_dados_produtos(db_path="data/ml_intelligence.db"):
    import pandas as pd

    """
    Conecta ao banco SQLite e retorna todos os dados da tabela 'products'
    como um DataFrame do Pandas.
    """
    # Verifica se o arquivo existe para evitar erro genérico de SQL
    if not os.path.exists(db_path):
        print(f"❌ Erro: O arquivo '{db_path}' não foi encontrado.")
        print("Verifique se você está rodando o notebook na raiz do projeto.")
        return None

    conn = sqlite3.connect(db_path)
    try:
        # Lê a tabela inteira e converte para DataFrame
        df = pd.read_sql_query("SELECT * FROM products", conn)
        
        # Opcional: Converter colunas de data que vêm como string
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            
        return df
    except Exception as e:
        print(f"Erro ao ler banco de dados: {e}")
        return None
    finally:
        conn.close()



def carregar_dados_vendedor(db_path="data/ml_intelligence.db"):
    import pandas as pd
    """
    Conecta ao banco SQLite e retorna todos os dados da tabela 'products'
    como um DataFrame do Pandas.
    """
    # Verifica se o arquivo existe para evitar erro genérico de SQL
    if not os.path.exists(db_path):
        print(f"❌ Erro: O arquivo '{db_path}' não foi encontrado.")
        print("Verifique se você está rodando o notebook na raiz do projeto.")
        return None

    conn = sqlite3.connect(db_path)
    try:
        # Lê a tabela inteira e converte para DataFrame
        df = pd.read_sql_query("SELECT * FROM sellers", conn)
        
        # Opcional: Converter colunas de data que vêm como string
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            
        return df
    except Exception as e:
        print(f"Erro ao ler banco de dados: {e}")
        return None
    finally:
        conn.close()