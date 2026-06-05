[README.md](https://github.com/user-attachments/files/28639714/README.md)
# Análise de E-commerce Brasileiro — Dataset Olist
### Processo Seletivo — Chico Rei

Projeto de análise de dados e pipeline ETL desenvolvido utilizando o dataset público da **Olist** — maior marketplace brasileiro — aplicado ao contexto de e-commerce da **Chico Rei**, maior loja de camisetas personalizadas do Brasil.

---

## Conteúdo do repositório

| Arquivo | Questão | Descrição |
|---|---|---|
| `analise_ecommerce_chicorei.ipynb` | Q11 | Análise estatística com Python, Pandas, Seaborn e Matplotlib |
| `etl_olist.py` | Q12 | Pipeline ETL completo: extração, transformação e carga em SQLite |

---

## Questão 11 — Análise Estatística (Jupyter Notebook)

Notebook com análise completa do dataset Olist respondendo três perguntas de negócio:

1. Como evoluíram as vendas ao longo do tempo e onde estão os melhores mercados?
2. Como os clientes pagam e qual o impacto do parcelamento no ticket médio?
3. A operação de entrega está performando bem?

**O que o notebook contém:**
- Leitura e inspeção de qualidade dos 5 arquivos CSV
- Estatísticas descritivas: média, mediana, desvio padrão e percentis
- Gráfico 1: Receita mensal + volume de pedidos (duplo eixo)
- Gráfico 2: Ticket médio por forma de pagamento (box plot + pizza)
- Gráfico 3: Receita e ticket médio por estado
- Gráfico 4: Distribuição do tempo de entrega e pontualidade por estado
- Resumo executivo com insights de negócio

**Principais resultados:**
- Receita total: R$ 12,2 milhões
- Ticket médio: R$ 127,57 | Ticket mediana: R$ ~80 (cliente típico)
- 91,9% dos pedidos entregues no prazo
- SP concentra ~38% da receita total
- Cartão de crédito representa ~74% dos pagamentos

### Como executar o notebook

```bash
# 1. Instalar dependências
pip install pandas numpy matplotlib seaborn

# 2. Baixar o dataset
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

# 3. Colocar os CSVs na mesma pasta do notebook

# 4. Abrir e executar
# Jupyter: Kernel → Restart and Run All
# VS Code: Run All Cells
```

**Arquivos CSV necessários:**
```
olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_customers_dataset.csv
olist_products_dataset.csv
```

---

## Questão 12 — Pipeline ETL (Python + SQLite)

Pipeline ETL profissional que extrai os dados brutos da Olist, aplica 18 transformações e carrega em um banco de dados SQLite estruturado.

**Arquitetura:**
```
[ CSV brutos ] → [ Extração ] → [ Transformação ] → [ Carga SQLite ] → [ Validação ]
```

**Transformações aplicadas:**
- Conversão de datas e colunas derivadas (dias de entrega, pontualidade)
- Filtro de outliers de preço (percentil 99.5)
- Remoção de duplicatas e registros inválidos
- Padronização de estados (uppercase) e cidades (title case)
- Tradução de formas de pagamento para português
- Mapeamento de categorias de produto para nomes legíveis
- Join das 5 tabelas em uma tabela fato de vendas

**Tabelas geradas no banco:**

| Tabela | Registros | Descrição |
|---|---|---|
| `fato_vendas` | 96.478 | Pedidos entregues com métricas financeiras e operacionais |
| `dim_clientes` | 96.096 | Clientes únicos com localização padronizada |
| `dim_produtos` | 32.951 | Produtos com categorias mapeadas |
| `dim_pagamentos` | 99.437 | Pagamentos agregados por pedido |

**Resultados da validação pós-carga:**
```
Total de pedidos:       96.478
Receita total:          R$ 12.242.181,11
Ticket médio:           R$ 127,57
Entregues no prazo:     91,9%
Estados cobertos:       27 (cobertura nacional)
Tempo de execução:      25,2 segundos
```

### Como executar o ETL

```bash
# 1. Instalar dependências
pip install pandas numpy

# 2. Colocar os CSVs na mesma pasta do script

# 3. Executar
python etl_olist.py

# 4. O banco olist_dw.db será gerado automaticamente
```

Para visualizar o banco gerado, use o [DB Browser for SQLite](https://sqlitebrowser.org) — gratuito e sem instalação de servidor.

---

## Dataset

**Olist Brazilian E-Commerce Public Dataset**  
Disponível em: [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)  
Licença: CC BY-NC-SA 4.0

---

## Tecnologias utilizadas

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.x-blue)
![Seaborn](https://img.shields.io/badge/Seaborn-latest-blue)
![SQLite](https://img.shields.io/badge/SQLite-3.x-blue)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange)
