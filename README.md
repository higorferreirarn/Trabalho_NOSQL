# Plataforma de Inteligência de Mercado em Tempo Real

Este projeto é um backend para uma Fintech que monitora cotações financeiras em tempo real, orquestrando **quatro bancos NoSQL** para diferentes propósitos: cache de baixa latência, data lake, série temporal e grafo de investidores.

## 🚀 Visão Geral

O sistema executa ciclos contínuos para:

- Consultar cotações do Dólar e Euro via API pública.
- Entregar o valor mais recente com baixíssima latência usando Redis.
- Armazenar logs brutos para auditoria no MongoDB (Data Lake).
- Registrar a série temporal de preços no Cassandra.
- Mapear e notificar investidores interessados usando Neo4j.

## 🏗️ Arquitetura

- **Redis:** Cache de cotações com TTL configurável.
- **MongoDB:** Armazena o documento bruto retornado pela API, incluindo timestamp.
- **Cassandra:** Guarda a série temporal das cotações para gráficos e análises históricas.
- **Neo4j:** Mantém a rede de investidores e suas moedas de interesse, simulando alertas.

## 📦 Pré-requisitos

- Python 3.8+
- Docker e Docker Compose (recomendado para subir os bancos)
- Bibliotecas Python listadas em `requirements.txt`

## 🐳 Subindo os Bancos com Docker Compose

Crie um arquivo `docker-compose.yml` semelhante a este:

```yaml
version: '3.8'
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"
  mongo:
    image: mongo:6
    ports:
      - "27017:27017"
  cassandra:
    image: cassandra:4
    ports:
      - "9042:9042"
  neo4j:
    image: neo4j:5
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      - NEO4J_AUTH=neo4j/higor1234

Suba os serviços:
  docker-compose up -d

Atenção: O script aguarda 120 segundos no início para garantir que todos os bancos estejam prontos.
```

## ⚙️ Instalação das Dependências

- pip install -r requirements.txt

## 📝 Como Funciona

Setup Inicial:

- O script conecta nos quatro bancos, cria estruturas e relacionamentos necessários automaticamente.

- Loop de Monitoramento:

A cada ciclo:

Verifica se a cotação está no Redis (cache hit). Se sim, usa o valor salvo.
Se não estiver (cache miss), consulta a API, atualiza o Redis e segue o fluxo.
Salva o documento bruto no MongoDB.
Insere o preço na tabela temporal do Cassandra.
Consulta no Neo4j quais investidores acompanham cada moeda e simula alerta (imprime no terminal), atualizando a propriedade ultima_notificacao no relacionamento.

Logs Visuais:

O terminal exibe claramente cada ação realizada em cada banco.

## 🏁 Executando o Script

- python monitor.py

## 📊 Exemplo de Saída no Terminal

[START] Aguardando 120 segundos para inicialização dos bancos de dados... [REDIS] Conectado com sucesso. [MONGO] Conectado com sucesso. [CASSANDRA] Keyspace e tabela prontos. [NEO4J] Grafo inicializado. --- Novo ciclo --- USD: R$ 5.1543 🟢 (Subiu) [REDIS] Cache HIT para USD: 5.1543 [MONGO] Cotação de USD salva no Data Lake. [CASSANDRA] Preço de USD gravado na série temporal. [NEO4J] Notificando investidores de USD: Alice, Bob, Carlos ...

## 👤 Investidores Simulados

- Os investidores são definidos estaticamente no código: INVESTIDORES = ['Alice', 'Bob', 'Carlos']
- Todos acompanham todas as moedas monitoradas.

## 🛠️ Customização
Para adicionar mais moedas, edite a lista MOEDAS.
Para adicionar mais investidores, edite a lista INVESTIDORES.

## 📚 Tecnologias Utilizadas

- Python
- Redis
- MongoDB
- Cassandra
- Neo4j

## 📄 Licença

- Este projeto foi criado para fins educacionais.

