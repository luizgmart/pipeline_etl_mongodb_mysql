📊 Pipeline ETL: API → MongoDB → MySQL

Este repositório contém um pipeline ETL (Extração, Transformação e Carga) que automatiza o fluxo de dados desde uma API externa até o MySQL, usando MongoDB como armazenamento intermediário.

O objetivo é demonstrar um fluxo completo de dados: pegar informações da API, armazenar temporariamente no MongoDB, aplicar transformações/limpezas e, finalmente, carregar os dados no MySQL para análises ou relatórios.

🚀 Funcionalidades

✔️ Extrai dados de uma API externa
✔️ Armazena os dados no MongoDB
✔️ Realiza transformações e limpezas nos dados
✔️ Carrega os dados transformados no MySQL
✔️ Scripts organizados para automação e reuso

📁 Estrutura do Repositório
pipeline_etl_mongodb_mysql/
├── data/                 # Dados de exemplo ou dumps temporários
├── scripts/              # Scripts Python para ETL e transformação
├── venv/                 # Ambiente virtual (não versionar)
├── .gitignore            # Ignora arquivos locais/temporários
├── README.md             # Documentação do projeto
