# Pipeline de Dados com Airflow + PostgreSQL

Pipeline ETL automatizado que coleta cotações de câmbio diariamente às 07:00 e persiste no banco de dados.

---

## Tecnologias

- **Docker + Docker Compose** — orquestração dos containers
- **Apache Airflow 2.8** — agendamento e execução do pipeline
- **PostgreSQL 15** — persistência dos dados
- **Python** — lógica das tarefas (ETL)

---

## Como subir o projeto

### Pré-requisitos
- Docker instalado
- Git instalado

### 1. Clonar o repositório

```bash
git clone https://github.com/SEU_USUARIO/pipeline-bi.git
cd pipeline-bi
```

### 2. Subir os containers

```bash
# Inicializa o banco do Airflow e cria o usuário admin
docker compose up airflow-init

# Aguarde aparecer: "admin user created successfully"
# Depois sobe todos os serviços em background

docker compose up -d
```

### 3. Verificar se tudo subiu

```bash
docker compose ps
```

Todos os serviços devem aparecer como `Up` ou `healthy`.

---

## Acessar o Airflow

Abra no navegador: **http://localhost:8080**

- Usuário: `admin`
- Senha: `admin`

> No GitHub Codespaces: vá na aba **PORTS**, mude a visibilidade da porta 8080 para **Public** e clique no link gerado.

---

## Executar a DAG

### Pela interface (recomendado)
1. Na lista de DAGs, localize `coleta_dados_bi`
2. Certifique-se que o toggle está **ativo** (azul)
3. Clique no botão ▶ **Trigger DAG**
4. Clique no nome da DAG para acompanhar a execução em tempo real

### Pelo terminal
```bash
docker compose exec airflow-webserver airflow dags trigger coleta_dados_bi
```

---

## Verificar o agendamento automático

A DAG está configurada para rodar **todos os dias às 07:00 UTC**.

Na interface do Airflow, na coluna **"Agendar"**, aparece `0 7 * * *` confirmando o agendamento.

---

## Consultar os dados no banco

Após a DAG rodar com sucesso, consulte os dados persistidos:

```bash
# Acessa o banco de BI
docker compose exec postgres-bi psql -U bi_user -d bi_database
```

Dentro do psql:

```sql
-- Cotações coletadas
SELECT moeda, valor_brl, coletado_em
FROM cotacoes
ORDER BY coletado_em DESC;

-- Histórico por moeda
SELECT moeda, COUNT(*) AS coletas, AVG(valor_brl) AS media
FROM cotacoes
GROUP BY moeda;

-- Log de execuções do pipeline
SELECT * FROM pipeline_log ORDER BY executado_em DESC;
```

---

## Estrutura do Projeto

```
pipeline-bi/
├── docker-compose.yml        # Define todos os containers
├── .env                      # Variáveis de ambiente
├── dags/
│   └── coleta_dados_dag.py   # DAG principal (ETL)
├── scripts/
│   └── init_db.sql           # Criação das tabelas
├── logs/                     # Logs gerados pelo Airflow
└── plugins/                  # Plugins customizados
```

---

## Fluxo do Pipeline (DAG)

```
[verificar_api] → [extrair_cotacoes] → [transformar_dados] → [carregar_banco] → [registrar_log]
```

| Tarefa | O que faz |
|--------|-----------|
| `verificar_api` | Testa se a API de câmbio está disponível |
| `extrair_cotacoes` | Busca cotações de USD, EUR, GBP e BTC |
| `transformar_dados` | Valida e limpa os dados recebidos |
| `carregar_banco` | Insere os dados no PostgreSQL |
| `registrar_log` | Registra o resultado da execução |

---

## Derrubar o ambiente

```bash
# Para os containers
docker compose down

# Para os containers E apaga os dados do banco
docker compose down -v
```
