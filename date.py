import psycopg2
import pandas as pd

def db(query: str, values: tuple = None) -> str | list | None:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='Blacknasa1337@@' port='5432'")
    cur = conn.cursor()
    if values:
        cur.execute(query, values)
    else:
        cur.execute(query)
    conn.commit()
    
    if query.lower().startswith("select"):
        result = cur.fetchall()
        return result
    return "Query executed successfully"
    conn.close()  


data = db("""
    CREATE TABLE IF NOT EXISTS mock_users (
        id INT PRIMARY KEY, 
        first_name VARCHAR(50), 
        last_name VARCHAR(50), 
        email VARCHAR(50), 
        gender VARCHAR(50), 
        ip_address VARCHAR(50)
    );
""")
print(data)


df = pd.read_csv("mock_data.csv")

for _, row in df.iterrows():
    insert_query = """
        INSERT INTO mock_users (id, first_name, last_name, email, gender, ip_address) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """
    message = db(insert_query, (row['id'], row['first_name'], row['last_name'], row['email'], row['gender'], row['ip_address']))
    print(message)
