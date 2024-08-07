import typesense
import psycopg2
import os

# Установите переменные окружения или измените эти значения на свои
DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://user:password@localhost:5432/marketplace')
TYPESENSE_HOST = os.getenv('TYPESENSE_HOST', 'localhost')
TYPESENSE_PORT = os.getenv('TYPESENSE_PORT', '8108')
TYPESENSE_API_KEY = os.getenv('TYPESENSE_API_KEY', 'xyz')

def create_typesense_client():
    """Создает клиент Typesense"""
    return typesense.Client({
        'nodes': [{
            'host': TYPESENSE_HOST,
            'port': TYPESENSE_PORT,
            'protocol': 'http'
        }],
        'api_key': TYPESENSE_API_KEY,
        'connection_timeout_seconds': 2
    })

def find_similar_products(client):
    collection = client.collections['products']
    search_parameters = {
        'q': '*',
        'query_by': 'description',
        'facet_by': 'brand,category',  # Указываем поля для фасетов
        'facet': True,  # Включаем фасеты
        'num_typos': 10,
        'max_candidates': 5
    }
    response = collection.documents.search(search_parameters)
    similar_products = [doc['id'] for doc in response['hits']]
    return similar_products

def update_similar_skus():
    conn = psycopg2.connect(
        host = 'localhost',
        port = '5432',
        user='admin',
        password='admin',
        database = 'offers'
    )
    cursor = conn.cursor()

    client = create_typesense_client()

    similar_products = find_similar_products(client)
    for product in products:
        similar_skus = find_similar_products({'title': product[1], 'description': product[2]})
        cursor.execute("""
            UPDATE sku
            SET similar_sku = %s
            WHERE uuid = %s
        """, (similar_skus, product[0]))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    update_similar_skus()
