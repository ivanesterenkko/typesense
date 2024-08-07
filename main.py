import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import Json
import uuid


def insert_offer(cursor, offer, categories):
    def get_category_levels(category_id):
        levels = []
        while category_id in categories:
            levels.append(categories[category_id]['name'])
            category_id = categories[category_id].get('parentId')
        levels.reverse()
        return levels + [None] * (3 - len(levels))

    # Проверка есть данные в XML или нет
    def get_text_or_default(element, default=None):
        return element.text if element is not None else default

    def get_float_or_default(element, default=0.0):
        try:
            return float(element.text) if element is not None else default
        except ValueError:
            return default

    def get_int_or_default(element, default=None):
        try:
            return int(element.text) if element is not None else default
        except ValueError:
            return default

    category_id = get_int_or_default(offer.find('categoryId'))
    category_levels = get_category_levels(category_id)
    category_remaining = ' / '.join(filter(None, category_levels[3:]))

    features = {param.attrib['name']: param.text for param in offer.findall('param')}

    cursor.execute("""
        INSERT INTO sku (uuid, marketplace_id, product_id, title, description, brand, seller_id, seller_name,
                         first_image_url, category_id, category_lvl_1, category_lvl_2, category_lvl_3,
                         category_remaining, features, rating_count, rating_value, price_before_discounts,
                         discount, price_after_discounts, bonuses, sales, currency, barcode)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING
    """, (
        str(uuid.uuid4()),  # uuid
        1,  # marketplace_id
        offer.get('id'),  # product_id
        get_text_or_default(offer.find('name')),  # title
        get_text_or_default(offer.find('description')),  # description
        get_text_or_default(offer.find('vendor')),  # brand
        None,  # seller_id
        get_text_or_default(offer.find('vendorCode')),  # seller_name
        get_text_or_default(offer.find('picture')),  # first_image_url
        category_id,  # category_id
        category_levels[0],  # category_lvl_1
        category_levels[1],  # category_lvl_2
        category_levels[2],  # category_lvl_3
        category_remaining,  # category_remaining
        Json(features),  # features
        None,  # rating_count
        None,  # rating_value
        get_float_or_default(offer.find('price')),  # price_before_discounts
        None,  # discount
        get_float_or_default(offer.find('price')),  # price_after_discounts
        None,  # bonuses
        None,  # sales
        get_text_or_default(offer.find('currencyId')),  # currency
        get_text_or_default(offer.find('barcode'))  # barcode
    ))
def main():
    conn = psycopg2.connect(
        host = 'localhost',
        port = '5432',
        user='admin',
        password='admin',
        database = 'offers'
    )
    cursor = conn.cursor()

    context = ET.iterparse('Электроника продукты 20240730.xml', events  =  ("end", ) )
    tree = ET.parse('Электроника продукты 20240730.xml')
    root = tree.getroot()

    categories = {}
    for category in root.find('shop/categories'):
        category_id = int(category.get('id'))
        parent_id = int(category.get('parentId')) if category.get('parentId') else None
        categories[category_id] = {'name': category.text, 'parentId': parent_id}

    for event, elem in context:
        if elem.tag == "offer":
            insert_offer(cursor, elem, categories)
            elem.clear()
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()