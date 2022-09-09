import json
import sys
import csv

# Creating database structure
def create_structure(connection):
    cursor = connection.cursor()
    
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS outlet(
            id_outlet VARCHAR(50) PRIMARY KEY, 
            name VARCHAR(30),
            address VARCHAR(30), 
            phone VARCHAR(20), 
            reviews_nr INTEGER, 
            country VARCHAR(56), 
            source VARCHAR(30)
            )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS users(
            user VARCHAR(50) PRIMARY KEY,
            reviews_nr INTEGER, 
            likes_nr INTEGER
            )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS reviews(
            review_body VARCHAR(300),
            rating FLOAT,
            user VARCHAR(50),
            id_outlet VARCHAR(50),
            FOREIGN KEY(user) REFERENCES users(user),
            FOREIGN KEY(id_outlet) REFERENCES outlet(id_outlet)
            )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS menu(
            product_price MONEY,
            product_volume INTEGER,
            product_name VARCHAR(20),
            product_brand VARCHAR(20),
            id_outlet VARCHAR(50),
            FOREIGN KEY(id_outlet) REFERENCES outlet(id_outlet)
            )"""
    )
    print("\n[USER STORY] Using sqlite3 and SQL queries (CREATE TABLE) in Python, I created the structure of SQLite database.")
    print("[USER STORY] The database includes 4 tables:")
    print("[USER STORY] (1) outlet (id_outlet as primary key), (2) users (user as primary key), (3) reviews (id_outlet, user as foreign keys), (4) menu (id_outlet as foreign key).")
    connection.commit()
    cursor.close()

# Check if data row exists in table
def exists_outlet(id_outlet, connection):
    cursor = connection.cursor()
    sql = "SELECT COUNT(*) FROM outlet WHERE id_outlet = ?"
    cursor.execute(sql, (id_outlet,))
    result  = cursor.fetchone()
    cursor.close()
    return result is not None and result[0] > 0

# Check if data row exists in table
def exists_user(user, connection):
    cursor = connection.cursor()
    sql = "SELECT COUNT(*) FROM users WHERE user = ?"
    cursor.execute(sql, (user,))
    result  = cursor.fetchone()
    cursor.close()
    return result is not None and result[0] > 0

# Check if data row exists in table
def exists_body(body, connection):
    cursor = connection.cursor()
    sql = "SELECT COUNT(*) FROM reviews WHERE review_body = ?"
    cursor.execute(sql, (body,))
    result  = cursor.fetchone()
    cursor.close()
    return result is not None and result[0] > 0

# Check if data row exists in table
def exists_name(name, connection):
    cursor = connection.cursor()
    sql = "SELECT COUNT(*) FROM menu WHERE product_name = ?"
    cursor.execute(sql, (name,))
    result  = cursor.fetchone()
    cursor.close()
    return result is not None and result[0] > 0

# Add a data row to a table
def add_outlet(connection, id_outlet, name, address, country, phone, reviews_nr, source):
    if exists_outlet(id_outlet, connection)==0:
        val = (id_outlet, name, address, country, phone, reviews_nr, source)
        cursor = connection.cursor()
        cursor.execute(
            """INSERT INTO outlet(
                id_outlet,
                name,
                address,
                country,
                phone,
                reviews_nr,
                source
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
            val,
        )
        connection.commit()
        cursor.close()

# Add a data row to a table
def add_users(connection, user, reviews_nr, likes_nr):
    if exists_user(user, connection)==0:
        val = (user, reviews_nr, likes_nr)
        cursor = connection.cursor()
        cursor.execute(
            """INSERT INTO users(
                user,
                reviews_nr,
                likes_nr            
                ) 
                VALUES (?, ?, ?)
                """,
            val,
        )
        connection.commit()
        cursor.close()

# Add a data row to a table
def add_reviews(connection, review_body, rating, user, id_outlet):
    if exists_body(review_body, connection)==0:
        val = (review_body, rating, user, id_outlet)
        cursor = connection.cursor()
        cursor.execute(
            """INSERT INTO reviews(
                review_body,
                rating,
                user,
                id_outlet            
                ) 
                VALUES (?, ?, ?, ?)
                """,
            val,
        )
        connection.commit()
        cursor.close()

# Add a data row to a table
def add_menu(connection, product_price, product_volume, product_name, product_brand, id_outlet):
    if exists_name(product_name, connection)==0:
        val = (product_price, product_volume, product_name, product_brand, id_outlet)
        cursor = connection.cursor()
        cursor.execute(
            """INSERT INTO menu(
                product_price, 
                product_volume,
                product_name, 
                product_brand, 
                id_outlet
                ) 
                VALUES (?, ?, ?, ?, ?)
                """,
            val,
        )
        connection.commit()
        cursor.close()
    
# Ingesting data from given JSON files to a table
def ingest_menu(connection):
    with open('./data/ubereats_menu.json') as f:
        data = json.load(f)
    for menu_data in data:        
        add_menu(connection,
            menu_data["price"],
            menu_data["volume"],
            menu_data["name"],
            menu_data["brand"],
            menu_data["id_outlet"]
        )
    print("[USER STORY] ubereats_menu.json loaded and ingested into menu table of casestudy.db ")

# Ingesting data from given JSON files to a table
def ingest_reviews(connection):
    with open('./data/tripadvisor_reviews.json') as f:
        data = json.load(f)
    for reviews_data in data:
        add_reviews(connection,
            reviews_data["body"],
            reviews_data["rating"],
            reviews_data["user"],
            reviews_data["id_outlet"]
        )
    print("[USER STORY] tripadvisor_reviews.json loaded and ingested into reviews table of casestudy.db ")

# Ingesting data from given JSON files to a table
def ingest_users(connection):
    with open('./data/tripadvisor_user.json') as f:
        data = json.load(f)
    for user_data in data:
        add_users(connection,
            user_data["user"],
            user_data["reviews"],
            user_data["likes"]
        )
    print("[USER STORY] tripadvisor_user.json loaded and ingested into user table of casestudy.db ")

# Ingesting data from given JSON files to a table
def ingest_outlets_from_TA(connection):
    with open('./data/tripadvisor_outlet.json') as f:
        data = json.load(f)
    for outlet_data in data:
        add_outlet(connection,
            outlet_data["id_outlet"],
            outlet_data["name"],
            outlet_data["address"],
            outlet_data["country"],
            outlet_data["phone"],
            outlet_data["reviews_nr"],
            "Tripadvisor"
        )
    print("[USER STORY] tripadvisor_outlet.json loaded and ingested into outlet table of casestudy.db ")

# Ingesting data from given JSON files to a table
def ingest_outlets_from_UE(connection):
    with open('./data/ubereats_outlet.json') as f:
        data = json.load(f)
    for outlet_data in data:    
        add_outlet(connection,
            outlet_data["id_outlet"],
            outlet_data["name"],
            outlet_data["address"],
            outlet_data["country"], 
            "NULL",
            outlet_data["reviews_nr"],
            "UberEats"
        )
    print("[USER STORY] ubereats_outlet.json loaded and ingested into outlet table of casestudy.db ")

# Example (or custom) query from the created database and saving query into a file
def example_query(connection):
    cursor = connection.cursor()
    csvWriter = csv.writer(open("output.csv", "w"), delimiter=",")
    try:
        sql = str(sys.argv[1])
        cursor.execute(sql)
        col_names = list(map(lambda x: x[0], cursor.description))
        print("\n[USER STORY] Result of the custom query (saved in output.csv):")
        print(col_names)
        csvWriter.writerow(col_names)
        rows = cursor.fetchall()  
        for row in rows:
            print(row)
            csvWriter.writerow(row)
    except:
        sql = "SELECT outlet.country, round(avg(reviews.rating), 2) avg_rating FROM outlet LEFT JOIN reviews ON reviews.id_outlet = outlet.id_outlet GROUP BY outlet.country;"
        cursor.execute(sql)
        col_names = list(map(lambda x: x[0], cursor.description))
        print("\n[USER STORY] Result of the example query (saved in output.csv):")
        print("\n",col_names, sep="")
        csvWriter.writerow(col_names)
        rows = cursor.fetchall()  
        for row in rows:
            print(row)
            csvWriter.writerow(row)
        print("\n[USER STORY] Example query:")
        print("[USER STORY] SELECT outlet.country, round(avg(reviews.rating), 2) avg_rating FROM outlet LEFT JOIN reviews ON reviews.id_outlet = outlet.id_outlet GROUP BY outlet.country;")
        print("[USER STORY] To commit a custom query use syntax: python3 run.py <SQL query in quotation marks>")
    