import time
import json
import requests
from datetime import datetime

# Importação dos drivers dos bancos NoSQL
import redis                # Redis para cache de baixa latência
from pymongo import MongoClient  # MongoDB para Data Lake
from cassandra.cluster import Cluster  # Cassandra para série temporal
from neo4j import GraphDatabase        # Neo4j para grafo de investidores

print("[START] Aguardando 120 segundos para inicialização dos bancos de dados...")
time.sleep(120)

# Configurações de conexão dos bancos
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

CASSANDRA_HOSTS = ['localhost']
CASSANDRA_KEYSPACE = 'fintech'
CASSANDRA_TABLE = 'historico_precos'

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "higor1234"

API_URL = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL"
TTL_SECONDS = 45  # Tempo de vida do cache no Redis (em segundos)

MOEDAS = ['USD', 'EUR']  # Moedas monitoradas
INVESTIDORES = ['Alice', 'Bob', 'Carlos']  # Investidores

# Função para conectar ao Redis
def setup_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()  # Testa conexão
        print("[REDIS] Conectado com sucesso.")
        return r
    except Exception as e:
        print(f"[REDIS] Erro de conexão: {e}")
        exit(1)

# Função para conectar ao MongoDB
def setup_mongo():
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client['fintech']
        print("[MONGO] Conectado com sucesso.")
        return db
    except Exception as e:
        print(f"[MONGO] Erro de conexão: {e}")
        exit(1)

# Função para conectar ao Cassandra e garantir keyspace/tabela
def setup_cassandra():
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect()
        # Cria keyspace se não existir
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
        """)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        # Cria tabela se não existir
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
                moeda text,
                data_coleta timestamp,
                valor decimal,
                PRIMARY KEY (moeda, data_coleta)
            ) WITH CLUSTERING ORDER BY (data_coleta DESC)
        """)
        print("[CASSANDRA] Keyspace e tabela prontos.")
        return session
    except Exception as e:
        print(f"[CASSANDRA] Erro de conexão: {e}")
        exit(1)

# Função para conectar ao Neo4j e garantir nós/relacionamentos iniciais
def setup_neo4j():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            # Cria nós de moedas e investidores, e relacionamentos [:ACOMPANHA]
            for moeda in MOEDAS:
                session.run("MERGE (:Moeda {nome: $nome})", nome=moeda)
            for investidor in INVESTIDORES:
                session.run("MERGE (:Investidor {nome: $nome})", nome=investidor)
                for moeda in MOEDAS:
                    session.run("""
                        MATCH (i:Investidor {nome: $investidor}), (m:Moeda {nome: $moeda})
                        MERGE (i)-[:ACOMPANHA]->(m)
                    """, investidor=investidor, moeda=moeda)
        print("[NEO4J] Grafo inicializado.")
        return driver
    except Exception as e:
        print(f"[NEO4J] Erro de conexão: {e}")
        exit(1)

# Função para buscar cotação na API externa
def get_price_from_api():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[API] Erro ao buscar cotação: {e}")
        return None

# Função para pegar o último preço salvo no Redis (para comparar volatilidade)
def get_last_price(redis_conn, moeda):
    cache_key = f"{moeda}_BRL"
    valor_cache = redis_conn.get(cache_key)
    if valor_cache:
        try:
            return float(json.loads(valor_cache)["bid"])
        except Exception:
            return None
    return None

# Função para imprimir a volatilidade da moeda no terminal
def print_volatilidade(moeda, preco_antigo, preco_novo):
    if preco_antigo is None:
        print(f"{moeda}: R$ {preco_novo:.4f} ⚪ (Primeira coleta)")
    elif preco_novo > preco_antigo:
        print(f"{moeda}: R$ {preco_novo:.4f} 🟢 (Subiu)")
    elif preco_novo < preco_antigo:
        print(f"{moeda}: R$ {preco_novo:.4f} 🔴 (Caiu)")
    else:
        print(f"{moeda}: R$ {preco_novo:.4f} 🟡 (Estável)")

# Função principal do monitoramento contínuo
def main():
    # Conecta aos bancos
    redis_conn = setup_redis()
    mongo_db = setup_mongo()
    cassandra_session = setup_cassandra()
    neo4j_driver = setup_neo4j()

    while True:
        print("\n--- Novo ciclo ---")
        now = datetime.now()  # Timestamp da coleta
        
        cotacoes = {}  # Dicionário para armazenar as cotações do ciclo
        
        for moeda in MOEDAS:
            cache_key = f"{moeda}_BRL"
            valor_cache = redis_conn.get(cache_key)
            if valor_cache:
                # Se tem cache válido no Redis, usa ele (cache hit)
                cotacoes[moeda] = json.loads(valor_cache)
                preco_atual = float(cotacoes[moeda]['bid'])
                print_volatilidade(moeda, preco_atual, preco_atual)
                print(f"[REDIS] Cache HIT para {moeda}: {cotacoes[moeda]['bid']}")
            else:
                # Cache miss: busca na API e atualiza Redis
                api_data = get_price_from_api()
                if not api_data or f"{moeda}BRL" not in api_data:
                    print(f"[API] Falha ao obter dados de {moeda}. Tentando novamente no próximo ciclo.")
                    continue
                key_api = f"{moeda}BRL"
                cotacao = {
                    "code": api_data[key_api]["code"],
                    "bid": api_data[key_api]["bid"],
                    "create_date": api_data[key_api]["create_date"]
                }
                preco_novo = float(cotacao["bid"])
                preco_antigo = get_last_price(redis_conn, moeda)
                print_volatilidade(moeda, preco_antigo, preco_novo)
                redis_conn.setex(cache_key, TTL_SECONDS, json.dumps(cotacao))
                cotacoes[moeda] = cotacao
                print(f"[REDIS] Atualizado cache para {moeda}.")

        # Salva cada cotação no MongoDB (Data Lake), Cassandra (série temporal) e notifica investidores no Neo4j
        for moeda, cotacao in cotacoes.items():
            doc = {
                "moeda": moeda,
                "valor": float(cotacao["bid"]),
                "data_coleta": now,
                "variacao": None,
                "payload_bruto": cotacao
            }
            mongo_db.cotacoes.insert_one(doc)
            print(f"[MONGO] Cotação de {moeda} salva no Data Lake.")

            # Salva na série temporal do Cassandra
            cql = f"""
                INSERT INTO {CASSANDRA_TABLE} (moeda, data_coleta, valor)
                VALUES (%s, %s, %s)
            """
            cassandra_session.execute(cql, (moeda, now, float(cotacao["bid"])))
            print(f"[CASSANDRA] Preço de {moeda} gravado na série temporal.")

            # Consulta investidores no Neo4j e atualiza ultima_notificacao no relacionamento [:ACOMPANHA]
            with neo4j_driver.session() as session:
                result = session.run("""
                    MATCH (i:Investidor)-[r:ACOMPANHA]->(m:Moeda {nome: $moeda})
                    RETURN i.nome AS nome_investidor
                """, moeda=moeda)
                nomes_investidores = [record["nome_investidor"] for record in result]
                if nomes_investidores:
                    print(f"[NEO4J] Notificando investidores de {moeda}: {', '.join(nomes_investidores)}")
                    for nome in nomes_investidores:
                        session.run("""
                            MATCH (i:Investidor {nome: $nome})-[r:ACOMPANHA]->(m:Moeda {nome: $moeda})
                            SET r.ultima_notificacao = $agora
                        """, nome=nome, moeda=moeda, agora=now.isoformat())
                else:
                    print(f"[NEO4J] Nenhum investidor acompanha {moeda}.")
        
        time.sleep(TTL_SECONDS)  # Aguarda o tempo do TTL antes do próximo ciclo

if __name__ == "__main__":
    main()