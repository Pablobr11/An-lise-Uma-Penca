"""
ETL Pipeline - Dataset Olist
Simulacao de processo ETL profissional
Extracao: CSV files (Olist public dataset)
Transformacao: limpeza, padronizacao, enriquecimento
Carga: banco de dados SQLite
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import logging
from datetime import datetime

# ─── Configuracao do logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ─── Parametros ────────────────────────────────────────────────────────────
PATH_CSV = './'           # pasta com os arquivos CSV do Olist
DB_PATH  = 'olist_dw.db'  # banco SQLite de destino

# ══════════════════════════════════════════════════════════════════════════
# ETAPA 1 — EXTRACAO (Extract)
# ══════════════════════════════════════════════════════════════════════════

def extract():
    """
    Le os arquivos CSV originais do dataset Olist sem nenhuma alteracao.
    Retorna um dicionario com os DataFrames brutos.
    """
    log.info("=== ETAPA 1: EXTRACAO ===")

    arquivos = {
        'orders':    'olist_orders_dataset.csv',
        'items':     'olist_order_items_dataset.csv',
        'payments':  'olist_order_payments_dataset.csv',
        'customers': 'olist_customers_dataset.csv',
        'products':  'olist_products_dataset.csv',
    }

    raw = {}
    for nome, arquivo in arquivos.items():
        caminho = os.path.join(PATH_CSV, arquivo)
        raw[nome] = pd.read_csv(caminho)
        log.info(f"  {arquivo}: {raw[nome].shape[0]:,} linhas x {raw[nome].shape[1]} colunas")

    return raw

# ══════════════════════════════════════════════════════════════════════════
# ETAPA 2 — TRANSFORMACAO (Transform)
# ══════════════════════════════════════════════════════════════════════════

def transform(raw: dict) -> dict:
    """
    Aplica todas as regras de limpeza, padronizacao e enriquecimento.
    Retorna um dicionario com as tabelas prontas para carga.
    """
    log.info("=== ETAPA 2: TRANSFORMACAO ===")

    # ── 2.1 Pedidos ──────────────────────────────────────────────────────
    log.info("  [2.1] Transformando pedidos...")
    orders = raw['orders'].copy()

    # Converter datas
    cols_data = [
        'order_purchase_timestamp',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
    ]
    for col in cols_data:
        orders[col] = pd.to_datetime(orders[col], errors='coerce')

    # Remover pedidos sem status definido
    antes = len(orders)
    orders = orders.dropna(subset=['order_status'])
    log.info(f"    Removidos {antes - len(orders)} pedidos sem status")

    # Colunas derivadas
    orders['ano_mes'] = orders['order_purchase_timestamp'].dt.to_period('M').astype(str)
    orders['dias_para_entrega'] = (
        orders['order_delivered_customer_date'] -
        orders['order_purchase_timestamp']
    ).dt.days

    orders['entregue_no_prazo'] = (
        orders['order_delivered_customer_date'] <=
        orders['order_estimated_delivery_date']
    ).astype('Int64')  # nullable int para nulos

    # Manter so pedidos entregues para a tabela fato
    orders_delivered = orders[orders['order_status'] == 'delivered'].copy()
    log.info(f"    Pedidos entregues: {len(orders_delivered):,} de {len(orders):,}")

    # ── 2.2 Itens ────────────────────────────────────────────────────────
    log.info("  [2.2] Transformando itens...")
    items = raw['items'].copy()

    # Remover itens com preco nulo ou negativo
    antes = len(items)
    items = items[(items['price'] > 0) & items['price'].notna()]
    log.info(f"    Removidos {antes - len(items)} itens com preco invalido")

    # Remover outliers extremos (acima do percentil 99.5)
    limite = items['price'].quantile(0.995)
    antes = len(items)
    items = items[items['price'] <= limite]
    log.info(f"    Removidos {antes - len(items)} outliers de preco (> R$ {limite:.2f})")

    # Receita total por item
    items['receita_item'] = items['price'] + items['freight_value']

    # ── 2.3 Pagamentos ───────────────────────────────────────────────────
    log.info("  [2.3] Transformando pagamentos...")
    payments = raw['payments'].copy()

    # Remover tipos invalidos
    tipos_validos = ['credit_card', 'boleto', 'voucher', 'debit_card']
    antes = len(payments)
    payments = payments[payments['payment_type'].isin(tipos_validos)]
    log.info(f"    Removidos {antes - len(payments)} pagamentos com tipo invalido")

    # Traduzir forma de pagamento para portugues
    mapa_pagamento = {
        'credit_card': 'Cartao de Credito',
        'boleto':      'Boleto',
        'voucher':     'Voucher',
        'debit_card':  'Cartao de Debito',
    }
    payments['forma_pagamento_pt'] = payments['payment_type'].map(mapa_pagamento)

    # Agregar por pedido (pode haver multiplas linhas de pagamento)
    payments_agg = payments.groupby('order_id').agg(
        valor_total_pago   = ('payment_value',        'sum'),
        max_parcelas       = ('payment_installments', 'max'),
        forma_pagamento_pt = ('forma_pagamento_pt',   'first'),
    ).reset_index()

    # ── 2.4 Clientes ─────────────────────────────────────────────────────
    log.info("  [2.4] Transformando clientes...")
    customers = raw['customers'].copy()

    # Padronizar estados (uppercase e strip)
    customers['customer_state'] = customers['customer_state'].str.upper().str.strip()
    customers['customer_city']  = customers['customer_city'].str.title().str.strip()

    # Remover duplicatas de customer_unique_id (manter mais recente pelo customer_id)
    antes = len(customers)
    customers = customers.drop_duplicates(subset='customer_unique_id', keep='last')
    log.info(f"    Removidos {antes - len(customers)} clientes duplicados")

    # ── 2.5 Produtos ─────────────────────────────────────────────────────
    log.info("  [2.5] Transformando produtos...")
    products = raw['products'].copy()

    # Preencher categoria nula com 'sem_categoria'
    products['product_category_name'] = (
        products['product_category_name']
        .fillna('sem_categoria')
        .str.lower()
        .str.strip()
    )

    # Mapeamento de categorias para o universo Chico Rei
    mapa_categoria = {
        'fashion_roupa_masculina':    'Moda Masculina',
        'fashion_roupa_feminina':     'Moda Feminina',
        'fashion_bolsas_e_acessorios':'Acessorios de Moda',
        'fashion_calcados':           'Calcados',
        'fashion_underwear_e_moda_praia': 'Moda Intima e Praia',
        'esporte_lazer':              'Esporte e Lazer',
        'artes':                      'Arte e Cultura',
        'brinquedos':                 'Brinquedos e Geek',
        'coisas_legais':              'Produtos Criativos',
        'papelaria':                  'Papelaria',
        'musica':                     'Musica',
        'livros_tecnicos':            'Livros',
        'livros_interesse_geral':     'Livros',
    }
    products['categoria_br'] = products['product_category_name'].map(mapa_categoria).fillna('Outros')

    # ── 2.6 Tabela Fato (join principal) ─────────────────────────────────
    log.info("  [2.6] Montando tabela fato de vendas...")

    # Agregar itens por pedido
    itens_por_pedido = items.groupby('order_id').agg(
        receita_produtos = ('price',         'sum'),
        receita_frete    = ('freight_value', 'sum'),
        receita_total    = ('receita_item',  'sum'),
        qtd_itens        = ('order_item_id', 'count'),
    ).reset_index()

    # Adicionar categoria predominante por pedido
    cat_pedido = (
        items.merge(products[['product_id','categoria_br']], on='product_id', how='left')
        .groupby('order_id')['categoria_br']
        .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else 'Outros')
        .reset_index()
        .rename(columns={'categoria_br': 'categoria_principal'})
    )

    # Montar fato final
    fato = (
        orders_delivered
        .merge(itens_por_pedido,                         on='order_id',   how='left')
        .merge(payments_agg,                             on='order_id',   how='left')
        .merge(customers[['customer_id','customer_state','customer_city',
                           'customer_unique_id']],       on='customer_id',how='left')
        .merge(cat_pedido,                               on='order_id',   how='left')
    )

    # Ticket medio para validacao
    ticket_medio = fato['receita_produtos'].mean()
    log.info(f"    Registros na fato: {len(fato):,}")
    log.info(f"    Ticket medio (validacao): R$ {ticket_medio:.2f}")

    # Selecionar e renomear colunas finais
    fato = fato[[
        'order_id', 'customer_unique_id', 'customer_state', 'customer_city',
        'ano_mes', 'order_purchase_timestamp',
        'receita_produtos', 'receita_frete', 'receita_total', 'qtd_itens',
        'dias_para_entrega', 'entregue_no_prazo',
        'forma_pagamento_pt', 'max_parcelas',
        'categoria_principal',
    ]].rename(columns={
        'order_purchase_timestamp': 'data_compra',
        'max_parcelas':             'parcelas',
        'forma_pagamento_pt':       'forma_pagamento',
    })

    return {
        'fato_vendas':   fato,
        'dim_clientes':  customers[['customer_unique_id','customer_state','customer_city']].drop_duplicates(),
        'dim_produtos':  products[['product_id','product_category_name','categoria_br']],
        'dim_pagamentos': payments_agg,
    }

# ══════════════════════════════════════════════════════════════════════════
# ETAPA 3 — CARGA (Load)
# ══════════════════════════════════════════════════════════════════════════

def load(transformed: dict) -> None:
    """
    Carrega as tabelas transformadas no banco de dados SQLite.
    Usa replace para permitir reexecucao idempotente do pipeline.
    """
    log.info("=== ETAPA 3: CARGA ===")

    conn = sqlite3.connect(DB_PATH)
    log.info(f"  Conectado ao banco: {DB_PATH}")

    for tabela, df in transformed.items():
        df.to_sql(tabela, conn, if_exists='replace', index=False)
        log.info(f"  Tabela '{tabela}' carregada: {len(df):,} registros")

    # Criar indices para performance de consulta
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_fato_state   ON fato_vendas(customer_state)",
        "CREATE INDEX IF NOT EXISTS idx_fato_anomes  ON fato_vendas(ano_mes)",
        "CREATE INDEX IF NOT EXISTS idx_fato_categ   ON fato_vendas(categoria_principal)",
        "CREATE INDEX IF NOT EXISTS idx_fato_pgto    ON fato_vendas(forma_pagamento)",
    ]
    for sql in indices:
        conn.execute(sql)
    log.info("  Indices criados com sucesso")

    conn.commit()
    conn.close()
    log.info(f"  Carga finalizada. Banco salvo em: {os.path.abspath(DB_PATH)}")

# ══════════════════════════════════════════════════════════════════════════
# VALIDACAO POS-CARGA
# ══════════════════════════════════════════════════════════════════════════

def validate() -> None:
    """
    Executa queries de validacao no banco carregado para confirmar
    a integridade dos dados.
    """
    log.info("=== VALIDACAO POS-CARGA ===")

    conn = sqlite3.connect(DB_PATH)

    queries = {
        "Total de pedidos na fato":
            "SELECT COUNT(*) FROM fato_vendas",
        "Receita total (R$)":
            "SELECT ROUND(SUM(receita_produtos), 2) FROM fato_vendas",
        "Ticket medio (R$)":
            "SELECT ROUND(AVG(receita_produtos), 2) FROM fato_vendas",
        "% Entregues no prazo":
            "SELECT ROUND(AVG(entregue_no_prazo)*100, 1) FROM fato_vendas WHERE entregue_no_prazo IS NOT NULL",
        "Estados distintos":
            "SELECT COUNT(DISTINCT customer_state) FROM fato_vendas",
        "Top estado por receita":
            "SELECT customer_state, ROUND(SUM(receita_produtos),2) AS receita FROM fato_vendas GROUP BY customer_state ORDER BY receita DESC LIMIT 1",
        "Forma de pagamento mais usada":
            "SELECT forma_pagamento, COUNT(*) AS qtd FROM fato_vendas GROUP BY forma_pagamento ORDER BY qtd DESC LIMIT 1",
    }

    for descricao, sql in queries.items():
        resultado = pd.read_sql(sql, conn)
        valor = resultado.iloc[0, 0] if len(resultado.columns) == 1 else resultado.to_string(index=False)
        log.info(f"  {descricao}: {valor}")

    conn.close()

# ══════════════════════════════════════════════════════════════════════════
# EXECUCAO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    inicio = datetime.now()
    log.info("Pipeline ETL iniciado")
    log.info(f"Timestamp: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("-" * 55)

    raw         = extract()
    transformed = transform(raw)
    load(transformed)
    validate()

    duracao = (datetime.now() - inicio).total_seconds()
    log.info("-" * 55)
    log.info(f"Pipeline concluido em {duracao:.1f} segundos")
