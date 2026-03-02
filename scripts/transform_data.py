from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")

def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

def select_category(col, category):
    query = { "Categoria do Produto": f"{category}"}
    return list(col.find(query))

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}
    return list(col.find(query))

def create_dataframe(lista):
    df = pd.DataFrame(lista)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")

if __name__ == "__main__":

    # Conexão MongoDB
    client = connect_mongo(MONGODB_URI)
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # Renomeando colunas
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # Filtrando categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    if "_id" in df_livros.columns:
        df_livros.rename(columns={"_id": "id"}, inplace=True)
    format_date(df_livros)
    save_csv(df_livros, "data/tabela_livros.csv")

    # Filtrando produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    if "_id" in df_produtos.columns:
        df_produtos.rename(columns={"_id": "id"}, inplace=True)
    format_date(df_produtos)
    save_csv(df_produtos, "data/tabela_produtos.csv")