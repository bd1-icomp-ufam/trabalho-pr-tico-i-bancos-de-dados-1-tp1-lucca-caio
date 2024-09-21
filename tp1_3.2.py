import psycopg2
import re

# Função para conectar ao banco de dados PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="tp1_db",  # Substitua pelo nome do seu banco de dados
            user="handler",  # Substitua pelo seu usuário
            password="123",  # Substitua pela sua senha
            host="localhost",  # Host, normalmente localhost
            port="5432"  # Porta do PostgreSQL, geralmente 5432
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para criar o esquema do banco de dados
def create_schema(conn):
    queries = [
        """
        CREATE TABLE IF NOT EXISTS Products (
            product_id SERIAL PRIMARY KEY,
            asin VARCHAR(20) UNIQUE NOT NULL,
            title TEXT,
            group_name VARCHAR(50),
            salesrank INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Categories (
            category_id INT PRIMARY KEY,
            category_name VARCHAR(255),
            parent_id INT REFERENCES Categories(category_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ProductCategories (
            product_id INT REFERENCES Products(product_id),
            category_id INT REFERENCES Categories(category_id),
            PRIMARY KEY (product_id, category_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SimilarProducts (
            product_id INT REFERENCES Products(product_id),
            similar_asin VARCHAR(20),
            PRIMARY KEY (product_id, similar_asin)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id VARCHAR(20) PRIMARY KEY
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Reviews (
            review_id SERIAL PRIMARY KEY,
            product_id INT REFERENCES Products(product_id),
            customer_id VARCHAR(20) REFERENCES Customers(customer_id),
            review_date DATE,
            rating INT,
            votes INT,
            helpful INT
        );
        """
    ]
    
    cur = conn.cursor()
    for query in queries:
        cur.execute(query)
    conn.commit()
    cur.close()

# Função para extrair os dados do arquivo de entrada
def extract_data_from_file(file_path):
    with open(file_path, 'r') as file:
        data = file.read()

    # Encontrar os blocos de cada produto
    product_blocks = re.findall(r'Id:.*?(?=\nId|\Z)', data, re.DOTALL)
    products = []

    for block in product_blocks:
        product = {}

        # Extrair ASIN
        asin_match = re.search(r'ASIN:\s*(\S+)', block)
        product['asin'] = asin_match.group(1) if asin_match else None

        # Extrair título
        title_match = re.search(r'title:\s*(.+)', block)
        product['title'] = title_match.group(1) if title_match else None

        # Extrair grupo
        group_match = re.search(r'group:\s*(\S+)', block)
        product['group'] = group_match.group(1) if group_match else None

        # Extrair ranking de vendas
        salesrank_match = re.search(r'salesrank:\s*(\d+)', block)
        product['salesrank'] = int(salesrank_match.group(1)) if salesrank_match else None

        # Extrair categorias com IDs
        categories_section = re.search(r'categories:\s*\d+', block)
        if categories_section:
            category_hierarchy = []
            categories_data = re.findall(r'\|(.+?)\n', block)  # Captura as categorias entre `|` e `\n`
            for category_path in categories_data:
                subcategories = []
                for cat in category_path.split('|'):
                    match = re.search(r'(.*)\[(\d+)\]', cat.strip())  # Capturar nome e ID da categoria
                    if match:
                        category_name = match.group(1).strip()
                        category_id = int(match.group(2).strip())
                        subcategories.append((category_name, category_id))  # Adicionar como (nome, ID)
                if subcategories:  # Certifique-se de que subcategories não está vazio
                    category_hierarchy.append(subcategories)
            product['categories'] = category_hierarchy
        else:
            product['categories'] = []

        # Extrair produtos similares
        similar_match = re.search(r'similar:\s*\d+\s+([\d\s]+)', block)
        if similar_match:
            similar_products = similar_match.group(1).strip().split()
            product['similar'] = similar_products
        else:
            product['similar'] = []

        # Extrair avaliações
        review_matches = re.findall(
            r'(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+([A-Z0-9]+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)', 
            block
        )
        product['reviews'] = [{
            'date': match[0],
            'customer': match[1],
            'rating': int(match[2]),
            'votes': int(match[3]),
            'helpful': int(match[4])
        } for match in review_matches]

        # Adicionar o produto à lista
        products.append(product)

    return products

# Função para popular o banco de dados com os dados extraídos
def populate_database(conn, products):
    cur = conn.cursor()

    # Dicionário para mapear categorias já inseridas e evitar duplicações
    category_map = {}

    def get_or_create_category(hierarchy):
        """
        Função para garantir que a hierarquia de categorias seja inserida corretamente
        com base nos IDs fornecidos.
        """
        parent_id = None
        for category_name, category_id in hierarchy:
            if category_id not in category_map:
                # Inserir a categoria no banco de dados com o ID fornecido
                cur.execute("""
                    INSERT INTO Categories (category_id, category_name, parent_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (category_id) DO NOTHING;
                """, (category_id, category_name, parent_id))
                category_map[category_id] = category_id

            parent_id = category_map[category_id]
        return category_map[hierarchy[-1][1]]  # Retorna o último ID da hierarquia

    for product in products:
        # Inserir o produto na tabela Products
        cur.execute("""
            INSERT INTO Products (asin, title, group_name, salesrank)
            VALUES (%s, %s, %s, %s)
            RETURNING product_id;
        """, (product['asin'], product['title'], product['group'], product['salesrank']))
        product_id = cur.fetchone()[0]

        # Inserir as categorias associadas ao produto
        for hierarchy in product['categories']:
            category_id = get_or_create_category(hierarchy)
            cur.execute("""
                INSERT INTO ProductCategories (product_id, category_id)
                VALUES (%s, %s);
            """, (product_id, category_id))

        # Inserir os produtos similares
        for similar_asin in product['similar']:
            cur.execute("""
                INSERT INTO SimilarProducts (product_id, similar_asin)
                VALUES (%s, %s);
            """, (product_id, similar_asin))

        # Inserir as avaliações
        for review in product['reviews']:
            cur.execute("""
                INSERT INTO Customers (customer_id)
                VALUES (%s)
                ON CONFLICT (customer_id) DO NOTHING;
            """, (review['customer'],))

            cur.execute("""
                INSERT INTO Reviews (product_id, customer_id, review_date, rating, votes, helpful)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (product_id, review['customer'], review['date'], review['rating'], review['votes'], review['helpful']))
        conn.commit()
    cur.close()

# Função principal para executar todo o processo
def main():
    conn = connect_to_db()
    if conn is None:
        return

    # Criar o esquema
    create_schema(conn)

    # Extrair os dados do arquivo de entrada
    file_path = "amazon-meta.txt"  # Insira o caminho correto para o arquivo de entrada
    products = extract_data_from_file(file_path)

    # Popular o banco de dados com os dados extraídos
    populate_database(conn, products)

    print("Dados inseridos com sucesso!")
    conn.close()

if __name__ == "__main__":
    main()