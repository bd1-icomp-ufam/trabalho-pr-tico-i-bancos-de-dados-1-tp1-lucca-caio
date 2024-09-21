import psycopg2
from datetime import datetime

# Função para conectar ao banco de dados PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="tp1_db",  # Substitua pelo nome do seu banco de dados
            user="dev",  # Substitua pelo seu usuário
            password="123",  # Substitua pela sua senha
            host="localhost",  # Host, normalmente localhost
            port="5432"  # Porta do PostgreSQL, geralmente 5432
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para listar os 5 comentários mais úteis com maior e menor avaliação
def get_top_and_bottom_reviews(conn, asin):
    cur = conn.cursor()

    # 5 Comentários mais úteis e com maior avaliação
    print("\n5 Comentários mais úteis e com maior avaliação:")
    cur.execute("""
        SELECT review_date, rating, votes, helpful
        FROM Reviews
        JOIN Products ON Reviews.product_id = Products.product_id
        WHERE asin = %s
        ORDER BY helpful DESC, rating DESC
        LIMIT 5;
    """, (asin,))
    reviews = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    for review in reviews:
        print(f"Data: {review[0]}, Avaliação: {review[1]}, Votos: {review[2]}, Útil: {review[3]}")

    # 5 Comentários mais úteis e com menor avaliação
    print("\n5 Comentários mais úteis e com menor avaliação:")
    cur.execute("""
        SELECT review_date, rating, votes, helpful
        FROM Reviews
        JOIN Products ON Reviews.product_id = Products.product_id
        WHERE asin = %s
        ORDER BY helpful DESC, rating ASC
        LIMIT 5;
    """, (asin,))
    reviews = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    for review in reviews:
        print(f"Data: {review[0]}, Avaliação: {review[1]}, Votos: {review[2]}, Útil: {review[3]}")

    cur.close()

# Função para listar produtos similares com maiores vendas
def get_similar_products_with_higher_sales(conn, asin):
    cur = conn.cursor()
    
    print("\nProdutos similares com maiores vendas:")
    cur.execute("""
        SELECT sp.similar_asin, p.title, p.salesrank
        FROM SimilarProducts sp
        JOIN Products p ON sp.similar_asin = p.asin
        WHERE sp.product_id = (SELECT product_id FROM Products WHERE asin = %s)
        AND p.salesrank < (SELECT salesrank FROM Products WHERE asin = %s)
        ORDER BY p.salesrank ASC;
    """, (asin, asin))
    similar_products = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    print(f"{'ASIN Similar':<20} {'Título':<100} {'Salesrank':<10}")
    for row in similar_products:
        print(f"{row[0]:<20} {row[1]:<100} {row[2]:<10}")


    cur.close()

# Função para mostrar a evolução diária das médias de avaliação
def get_rating_evolution(conn, asin):
    cur = conn.cursor()

    # Evolução diária das médias de avaliação
    print("\nEvolução diária das médias de avaliação:")
    cur.execute("""
        SELECT review_date, AVG(rating) AS avg_rating
        FROM Reviews
        WHERE product_id = (SELECT product_id FROM Products WHERE asin = %s)
        GROUP BY review_date
        ORDER BY review_date ASC;
    """, (asin,))
    average_ratings = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    print(f"{'Data':<15} {'Média de Avaliação':<20}")
    for row in average_ratings:
        print(f"{row[0]}      {row[1]:<20.2f}")

    cur.close()

# Função para listar os 10 produtos líderes de venda em cada grupo
def get_top_sales_per_group(conn):
    cur = conn.cursor()

    # 10 produtos líderes de venda em cada grupo
    print("\n10 produtos líderes de venda em cada grupo:")
    cur.execute("""
        WITH RankedProducts AS (
            SELECT group_name, asin, title, salesrank,
                ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY salesrank ASC) as rank
            FROM Products
            WHERE salesrank IS NOT NULL
        )
        SELECT group_name, asin, title, salesrank
        FROM RankedProducts
        WHERE rank <= 10
        ORDER BY group_name, salesrank;
    """)
    top_products = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    print(f"{'Grupo':<20} {'ASIN':<20} {'Título':<100} {'Salesrank':<10}")
    for row in top_products:
        print(f"{row[0]:<20} {row[1]:<20} {row[2]:<100} {row[3]:<10}")

    cur.close()

# Função para listar os 10 produtos com a maior média de avaliações úteis positivas
def get_top_helpful_reviews(conn):
    cur = conn.cursor()

    # 10 produtos com a maior média de avaliações úteis positivas
    print("\n10 produtos com a maior média de avaliações úteis positivas:")
    cur.execute("""
        SELECT p.asin, p.title, AVG(r.helpful) AS avg_helpful
        FROM Reviews r
        JOIN Products p ON r.product_id = p.product_id
        GROUP BY p.asin, p.title
        ORDER BY avg_helpful DESC
        LIMIT 10;
    """)
    top_helpful_products = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    print(f"{'ASIN':<20} {'Título':<100} {'Média de Avaliações Úteis':<25}")
    for row in top_helpful_products:
        print(f"{row[0]:<20} {row[1]:<100} {row[2]:<25.2f}")

    cur.close()

# Função para listar as 5 categorias com a maior média de avaliações úteis positivas
def get_top_categories_by_helpful_reviews(conn):
    cur = conn.cursor()

    # 5 categorias com a maior média de avaliações úteis positivas
    print("\n5 categorias com a maior média de avaliações úteis positivas:")
    cur.execute("""
        SELECT c.category_name, AVG(r.helpful) AS avg_helpful
        FROM Reviews r
        JOIN ProductCategories pc ON r.product_id = pc.product_id
        JOIN Categories c ON pc.category_id = c.category_id
        GROUP BY c.category_name
        ORDER BY avg_helpful DESC
        LIMIT 5;
    """)
    top_categories = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    print(f"{'Categoria':<50} {'Média de Avaliações Úteis':<25}")
    for row in top_categories:
        print(f"{row[0]:<50} {row[1]:<25.2f}")

    cur.close()

# Função para listar os 10 clientes que mais fizeram comentários por grupo de produto
def get_top_customers_by_reviews(conn):
    cur = conn.cursor()

    # 10 clientes que mais fizeram comentários por grupo de produto
    print("\n10 clientes que mais fizeram comentários por grupo de produto:")
    cur.execute("""
        WITH RankedCustomers AS (
            SELECT p.group_name, r.customer_id, COUNT(*) AS total_reviews,
                ROW_NUMBER() OVER (PARTITION BY p.group_name ORDER BY COUNT(*) DESC) as rank
            FROM Reviews r
            JOIN Products p ON r.product_id = p.product_id
            GROUP BY p.group_name, r.customer_id
        )
        SELECT group_name, customer_id, total_reviews
        FROM RankedCustomers
        WHERE rank <= 10
        ORDER BY group_name, total_reviews DESC;
    """)
    top_customers = cur.fetchall()

    # Exibir resultados com os nomes dos atributos
    print(f"{'Grupo':<20} {'Cliente ID':<20} {'Total de Avaliações':<25}")
    for row in top_customers:
        print(f"{row[0]:<20} {row[1]:<20} {row[2]:<25}")

    cur.close()

# Função principal para o dashboard
def dashboard():
    conn = connect_to_db()
    if conn is None:
        return

    while True:
        print("\n--- Dashboard ---")
        print("1. Listar 5 comentários mais úteis e com maior/menor avaliação")
        print("2. Listar produtos similares com maiores vendas")
        print("3. Mostrar evolução diária das médias de avaliação")
        print("4. Listar 10 produtos líderes de venda em cada grupo")
        print("5. Listar 10 produtos com maior média de avaliações úteis positivas")
        print("6. Listar 5 categorias com maior média de avaliações úteis positivas")
        print("7. Listar 10 clientes que mais fizeram comentários por grupo")
        print("0. Sair")

        choice = input("Escolha uma opção: ")

        if choice == '1':
            asin = input("Digite o ASIN do produto: ")
            get_top_and_bottom_reviews(conn, asin)
        elif choice == '2':
            asin = input("Digite o ASIN do produto: ")
            get_similar_products_with_higher_sales(conn, asin)
        elif choice == '3':
            asin = input("Digite o ASIN do produto: ")
            get_rating_evolution(conn, asin)
        elif choice == '4':
            get_top_sales_per_group(conn)
        elif choice == '5':
            get_top_helpful_reviews(conn)
        elif choice == '6':
            get_top_categories_by_helpful_reviews(conn)
        elif choice == '7':
            get_top_customers_by_reviews(conn)
        elif choice == '0':
            break
        else:
            print("Opção inválida, tente novamente!")

    conn.close()

if __name__ == "__main__":
    dashboard()
