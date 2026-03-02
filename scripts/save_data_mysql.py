import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd


# Carregar variáveis do .env

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# Funções MySQL

def connect_mysql(DB_HOST, DB_USERNAME, DB_PASSWORD):
    cnx = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD
    )
    print("Conectado ao MySQL:", cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"\nBase de dados {db_name} criada ou já existia")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for x in cursor:
        print(x)

def create_product_table(cursor, db_name, tb_name):    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
            id VARCHAR(100),
            Produto VARCHAR(100),
            Categoria_Produto VARCHAR(100),
            Preco FLOAT(10,2),
            Frete FLOAT(10,2),
            Data_Compra DATE,
            Vendedor VARCHAR(100),
            Local_Compra VARCHAR(100),
            Avaliacao_Compra INT,
            Tipo_Pagamento VARCHAR(100),
            Qntd_Parcelas INT,
            Latitude FLOAT(10,2),
            Longitude FLOAT(10,2),
            PRIMARY KEY (id)
        );
    """)
    print(f"\nTabela {tb_name} criada ou já existia")

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)

# Ler CSV

def read_csv(path):
    df = pd.read_csv(path)
    # Se tiver '_id', renomear para 'id'
    if "_id" in df.columns:
        df.rename(columns={"_id": "id"}, inplace=True)
    return df

# Inserir dados

def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista = [tuple(row) for _, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql, lista)
    cnx.commit()
    print(f"\n{cursor.rowcount} dados foram inseridos na tabela {tb_name}.")


# Execução principal

if __name__ == "__main__":
    
    # Conexão MySQL usando .env
    cnx = connect_mysql(DB_HOST, DB_USER, DB_PASSWORD)
    cursor = create_cursor(cnx)

    # Criar banco e tabela
    create_database(cursor, "db_produtos_teste")
    show_databases(cursor)
    create_product_table(cursor, "db_produtos_teste", "tb_livros")
    show_tables(cursor, "db_produtos_teste")

    # Ler CSV e inserir dados
    df = read_csv("data/tabela_livros.csv")
    add_product_data(cnx, cursor, df, "db_produtos_teste", "tb_livros")