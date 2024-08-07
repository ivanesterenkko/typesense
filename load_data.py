
import psycopg2
import typesense

def upload_data_to_typesense(client, rows):
    batch = [{'uuid': row[0], 'title': row[1], 'description': row[2]} for row in rows]
    
    client.collections['products'].documents.import_(batch, {'action': 'upsert'})

def find_similar_products(client, product):
    collection = client.collections['products']
    search_parameters = {
        'q': product['title'],
        'query_by': 'description',
        'max_candidates': 5
    }
    similar_products = []
    response = collection.documents.search(search_parameters)
    for doc in response['hits']:
        if doc.count('uuid') > 0:
            similar_products.apend( doc['uuid'])
    return similar_products

def main():
    conn = psycopg2.connect(
        host = 'localhost',
        port = '5432',
        user='admin',
        password='admin',
        database = 'offers'
    )
    cursor = conn.cursor()

    query = """
        SELECT
            uuid, title, description
        FROM
            public.sku
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    # Создание клиента Typesense
    client = typesense.Client({
    'nodes': [{
        'host': 'localhost',  # For Typesense Cloud use xxx.a1.typesense.net
        'port': '8108',       # For Typesense Cloud use 443
        'protocol': 'http'    # For Typesense Cloud use https
    }],
    'api_key': 'xyz',
    'connection_timeout_seconds': 2
    })
    schema = {
        'name': 'products',
        'fields': [
            {'name': 'uuid', 'type': 'string', 'facet': True},
            {'name': 'title', 'type': 'string', 'facet': True},
            {'name': 'description', 'type': 'string', 'facet': True},
        ]
    }
    client.collections['products'].delete()
    client.collections.create(schema)
    for row in rows:
        batch = [{'uuid': row[0], 'title': row[1], 'description': row[2]} for row in rows]
        client.collections['products'].documents.import_(batch, {'action': 'upsert'})
    cursor.execute("SELECT uuid, title, description FROM public.sku")
    products = cursor.fetchall()
    for product in products:
        uuid, title, description = product
        similar_products = find_similar_products(client, {'title': title, 'description': description})
        cursor.execute("""
            UPDATE sku
            SET similar_sku = %s
            WHERE uuid = %s
        """, (similar_products, uuid))
        print(uuid,similar_products)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
