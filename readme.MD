# Scraper ML

Sistema de web scraping e inteligência de mercado para o Mercado Livre. Utiliza **Python**, **Playwright** e **SQLite** para monitoramento de preços, análise de concorrência e extração de detalhes técnicos.

O sistema opera em dois estágios principais: Busca (Search) para descobrir produtos e Enriquecimento (Detail) para extrair dados profundos (descrição, características técnicas, histórico de reviews e dados do vendedor).

## 📋 Funcionalidades

- **Busca Automatizada**: Varre múltiplas páginas de busca por termos específicos e coleta, volume de vendas, posicao, avaliação, etc.
- **Extração de Detalhes (Deep Crawl):** Acessa a página de cada produto para coletar:

    - Ficha técnica completa (JSON).
    - Descrição completa e Resumo IA (se disponível).
    - Dados do Vendedor (Nível, Vendas totais, Loja Oficial). 
    - Análise de Reviews: Coleta a data do último review e contagem de reviews nos últimos 90 dias para medir a "saúde" e velocidade de vendas do produto.

- **Anti-Bot & Stealth**: Utiliza rotação de User-Agents e remove flags do WebDriver para evitar bloqueios.
- **Banco de Dados Estruturado**: Salva tudo em SQLite relacional (products e sellers).
- **Filtros Inteligentes**: Permite enriquecer apenas produtos que atendam a critérios (ex: Preço mínimo, Nota mínima, Apenas novos).

## 📂 Estrutura do Projeto
```
.
├── main.py                 # Ponto de entrada (CLI)
├── src/
│   ├── database.py         # Gerenciamento do SQLite e Models
│   ├── search_scraper.py   # Bot de Busca (Lista de produtos)
│   └── detail_scraper.py   # Bot de Detalhes (Página do produto)
├── data/
│   └── ml_intelligence.db  # Banco de dados gerado (automático)
└── README.md
```

## 🛠 Instalação

Requer Python 3.8+.

```bash
# 1. Instale as dependências
pip install playwright pandas

# 2. Instale os binários do navegador
playwright install chromium
```

##  🚀 Como Usar
O ponto de entrada é o arquivo main.py.

###  1. Modo Busca (`search`)
Apenas descobre produtos e salva dados básicos (Preço, Link, ID, Posição).

```bash
python main.py --mode search --terms "iphone 15" "samsung s24" --pages 3
```
### 2. Modo Detalhe (`detail`)
Percorre o banco de dados para enriquecer produtos já descobertos.

```bash
# Detalhar apenas produtos novos (nunca visitados)
python main.py --mode detail --only-new --limit 50

# Detalhar produtos acima de R$ 100 com nota mínima 4.5
python main.py --mode detail --min-price 100 --min-rating 4.5
```

### 3. Modo Completo (`full`)
Executa a busca e, em seguida, o detalhamento dos itens encontrados.

```bash
python main.py --mode full --terms "cadeira gamer" --pages 2 --limit 20
```

### ⚙️ Argumentos da CLI

| Argumento | Descrição | Padrão |
| :--- | :--- | :--- |
| `--mode` | `search`, `detail` ou `full`. | `full` |
| `--terms` | Termos para busca (Obrigatório em `search`/`full`). | - |
| `--pages` | Páginas a percorrer por termo na busca. | `3` |
| `--limit` | Limite de produtos a processar no modo detalhe. | `20` |
| `--only-new` | Flag: Processa apenas itens sem detalhes no banco. | `False` |
| `--min-price` | Filtro: Preço mínimo para enriquecimento. | `0.0` |
| `--days-since-update`| Filtro: Reprocessar itens não atualizados há X dias. | `0` |

## 📂 Estrutura do Banco de Dados

Os dados são salvos em `data/ml_intelligence.db`.

- Tabela `products`: Contém dados de busca (`price`, `permalink`) e detalhes (`specifications_json`, `comments_last_90d`, `ai_summary`).

- Tabela `sellers`: Contém reputação e histórico de vendas dos vendedores.

Para carregar os dados em Pandas:

```python
from src.database import carregar_dados_produtos,  carregar_dados_vendedor

df_p = carregar_dados_produtos()
df_s = carregar_dados_vendedor()
```

## ⚠️ Aviso Legal
Este software foi compartilhado para fins educacionais e portifolio. O uso excessivo de scrapers pode violar os Termos de Serviço do Mercado Livre.

- Use com moderação (respeite os time.sleep implementados).
- Não utilize para ataques de negação de serviço.
- **O autor não se responsabiliza** pelo uso indevido da ferramenta.
- Use pelo seu prórpio risco e responsabilidade


