from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")


# Conexão MongoDB

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Conectado com sucesso ao MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_connect_db(client, db_name):
    return client[db_name]

def create_connect_collection(db, col_name):
    return db[col_name]


# Extração e inserção de dados

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    # Insere diretamente sem usar _id da API
    docs = col.insert_many(data)
    return len(docs.inserted_ids)


# Exportação para CSV

def create_dataframe(col, query={}):
    df = pd.DataFrame(list(col.find(query)))
    if "_id" in df.columns:
        df.rename(columns={"_id": "id"}, inplace=True)
    return df

def format_date(df):
    if "Data da Compra" in df.columns:
        df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], errors="coerce", dayfirst=True)
        df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"CSV salvo em {path}")


# Execução principal

if __name__ == "__main__":
    client = connect_mongo(MONGODB_URI)
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # Extrair dados da API e inserir/upserta
    data = extract_api_data("https://labdados.com/produtos")
    print(f"Quantidade de dados extraídos: {len(data)}")
    n_docs = insert_data(col, data)
    print(f"Quantidade de documentos inseridos/atualizados: {n_docs}")

    # Exportar CSV categoria livros
    df_livros = create_dataframe(col, {"Categoria do Produto": "livros"})
    format_date(df_livros)
    save_csv(df_livros, "data/tabela_livros.csv")

    # Exportar CSV produtos vendidos a partir de 2021
    df_produtos = create_dataframe(col, {"Data da Compra": {"$regex": "/202[1-9]"}})
    format_date(df_produtos)
    save_csv(df_produtos, "data/tabela_2021_em_diante.csv")

    client.close()
