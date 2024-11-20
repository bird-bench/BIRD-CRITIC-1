import pymysql


def connect_mysql(db_name):
    # Connect to the Docker-hosted MySQL database
    db = pymysql.connect(
        host="bird_critic_mysql",  # Hostname for accessing MySQL service within Docker network
        user="root",
        password="123123",
        database=db_name,
        port=3306,
    )
    return db


def execute_mysql_query(cursor, query):
    """Execute a MySQL query."""
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def perform_query_on_mysql_databases(query, db_name):
    db = connect_mysql(db_name)
    cursor = db.cursor()
    result = execute_mysql_query(cursor, query)
    db.close()
    return result


if __name__ == "__main__":
    # Define a sample query, like fetching table names from a specific database
    sample_query = "SHOW TABLES;"
    db_name = "superhero"  # Replace with the actual database name

    # Execute the query
    try:
        results = perform_query_on_mysql_databases(sample_query, db_name)
        print("Query Results:", results)
    except Exception as e:
        print("An error occurred:", e)
