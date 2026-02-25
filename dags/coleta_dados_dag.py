from datetime import datetime, timedelta
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import os

default_args = {
    'owner': 'equipe-bi',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='coleta_dados_bi',
    default_args=default_args,
    description='Coleta diária de cotações para o BI',
    schedule_interval='0 7 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bi', 'cotacoes', 'diario'],
) as dag:

    def get_engine():
        conn_str = os.environ.get(
            'BI_DB_CONN',
            'postgresql+psycopg2://bi_user:bi_senha123@postgres-bi/bi_database'
        )
        return create_engine(conn_str)

    def verificar_api(**context):
        logging.info("Verificando disponibilidade da API...")
        url = "https://economia.awesomeapi.com.br/json/last/USD-BRL"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        logging.info(f"API disponível. Status: {resp.status_code}")
        context['ti'].xcom_push(key='api_status', value='ok')

    def extrair_cotacoes(**context):
        logging.info("Iniciando extração de cotações...")
        moedas = ['USD-BRL', 'EUR-BRL', 'GBP-BRL', 'BTC-BRL']
        url = f"https://economia.awesomeapi.com.br/json/last/{','.join(moedas)}"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        dados = resp.json()
        cotacoes = []
        for chave, info in dados.items():
            cotacoes.append({
                'moeda': info['code'],
                'valor_brl': float(info['bid']),
                'coletado_em': datetime.now().isoformat()
            })
            logging.info(f"  {info['code']}: R$ {info['bid']}")
        context['ti'].xcom_push(key='cotacoes', value=cotacoes)
        return cotacoes

    def transformar_dados(**context):
        cotacoes = context['ti'].xcom_pull(task_ids='extrair_cotacoes', key='cotacoes')
        if not cotacoes:
            raise ValueError("Nenhum dado recebido!")
        cotacoes_validas = []
        for c in cotacoes:
            if c['valor_brl'] <= 0:
                logging.warning(f"Valor inválido: {c}")
                continue
            c['valor_brl'] = round(c['valor_brl'], 4)
            cotacoes_validas.append(c)
        logging.info(f"Dados válidos: {len(cotacoes_validas)}")
        context['ti'].xcom_push(key='cotacoes_validas', value=cotacoes_validas)

    def carregar_banco(**context):
        cotacoes = context['ti'].xcom_pull(task_ids='transformar_dados', key='cotacoes_validas')
        if not cotacoes:
            raise ValueError("Nenhum dado para carregar!")
        engine = get_engine()
        inseridos = 0
        with engine.connect() as conn:
            for c in cotacoes:
                sql = text("""
                    INSERT INTO cotacoes (moeda, valor_brl, coletado_em)
                    VALUES (:moeda, :valor_brl, :coletado_em)
                """)
                conn.execute(sql, c)
                inseridos += 1
            conn.commit()
        logging.info(f"Carregados {inseridos} registros.")
        context['ti'].xcom_push(key='total_inserido', value=inseridos)

    def registrar_log(**context):
        total = context['ti'].xcom_pull(task_ids='carregar_banco', key='total_inserido')
        engine = get_engine()
        with engine.connect() as conn:
            sql = text("""
                INSERT INTO pipeline_log (dag_id, status, mensagem)
                VALUES (:dag_id, :status, :mensagem)
            """)
            conn.execute(sql, {
                'dag_id': 'coleta_dados_bi',
                'status': 'SUCESSO',
                'mensagem': f'{total} cotações inseridas.'
            })
            conn.commit()
        logging.info("Log registrado.")

    t1 = PythonOperator(task_id='verificar_api',      python_callable=verificar_api)
    t2 = PythonOperator(task_id='extrair_cotacoes',   python_callable=extrair_cotacoes)
    t3 = PythonOperator(task_id='transformar_dados',  python_callable=transformar_dados)
    t4 = PythonOperator(task_id='carregar_banco',     python_callable=carregar_banco)
    t5 = PythonOperator(task_id='registrar_log',      python_callable=registrar_log)

    t1 >> t2 >> t3 >> t4 >> t5
