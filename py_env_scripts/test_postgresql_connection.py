import psycopg2


def connect_postgresql(db_name):
    # Connect to the Docker-hosted PostgreSQL database
    db = psycopg2.connect(
        dbname=db_name,
        user="root",  # PostgreSQL user in Docker setup
        host="bird_critic_postgresql",  # Docker host (assuming exposed on bird_critic_postgresql)
        password="123123",  # Password used in Docker PostgreSQL setup
        port="5432",  # Port exposed by Docker in docker-compose.yml
    )
    return db


def execute_postgresql_query(cursor, query):
    """Execute a PostgreSQL query."""
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def perform_query_on_postgresql_databases(query, db_name):
    db = connect_postgresql(db_name)
    cursor = db.cursor()
    result = execute_postgresql_query(cursor, query)
    db.close()
    return result


if __name__ == "__main__":
    # Define a sample query, like fetching table names from a specific database
    sample_query = "SELECT * FROM colour;"
    db_name = "superhero"  # Replace with the actual database name

    # Execute the query
    try:
        results = perform_query_on_postgresql_databases(sample_query, db_name)
        print("Query Results:", results)
    except Exception as e:
        print("An error occurred:", e)
