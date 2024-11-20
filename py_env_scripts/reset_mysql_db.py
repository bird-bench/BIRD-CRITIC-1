import pymysql
import sqlparse
import os
import sys
import logging
import argparse
import datetime

# Define database-to-table mapping
DATABASE_MAPPING = {
    "debit_card_specializing": [
        "customers",
        "gasstations",
        "products",
        "yearmonth",
        "transactions_1k",
    ],
    "financial": [
        "loan",
        "client",
        "district",
        "trans",
        "account",
        "card",
        "order",
        "disp",
    ],
    "formula_1": [
        "circuits",
        "status",
        "drivers",
        "driverStandings",
        "races",
        "constructors",
        "constructorResults",
        "lapTimes",
        "qualifying",
        "pitStops",
        "seasons",
        "constructorStandings",
        "results",
    ],
    "california_schools": ["schools", "satscores", "frpm"],
    "card_games": [
        "legalities",
        "cards",
        "rulings",
        "set_translations",
        "sets",
        "foreign_data",
    ],
    "european_football_2": [
        "Team_Attributes",
        "Player",
        "Match",
        "League",
        "Country",
        "Player_Attributes",
        "Team",
    ],
    "thrombosis_prediction": ["Laboratory", "Patient", "Examination"],
    "toxicology": ["bond", "molecule", "atom", "connected"],
    "student_club": [
        "income",
        "budget",
        "zip_code",
        "expense",
        "member",
        "attendance",
        "event",
        "major",
    ],
    "superhero": [
        "gender",
        "superpower",
        "publisher",
        "superhero",
        "colour",
        "attribute",
        "hero_power",
        "race",
        "alignment",
        "hero_attribute",
    ],
    "codebase_community": [
        "postLinks",
        "postHistory",
        "badges",
        "posts",
        "users",
        "tags",
        "votes",
        "comments",
    ],
}


def reset_and_restore_database(db_name, logger):
    """Delete, recreate the database, and restore tables based on the mapping."""
    try:
        # Connect to MySQL server
        connection = pymysql.connect(
            host="bird_critic_mysql",  # Hostname for Docker network
            user="root",
            password="123123",
            port=3306,
        )

        with connection.cursor() as cursor:
            # Delete the database if it exists
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
            logger.info(f"Database {db_name} deleted.")

            # Recreate the database
            cursor.execute(f"CREATE DATABASE {db_name};")
            logger.info(f"Database {db_name} recreated.")

        connection.commit()

        # Set the context to the database
        with connection.cursor() as cursor:
            cursor.execute(f"USE {db_name};")
            logger.info(f"Using database {db_name}.")

        # Restore tables from the SQL dump files
        table_names = DATABASE_MAPPING.get(db_name.lower(), [])
        for table in table_names:
            dump_file_path = f"./mysql_table_dumps/{table}.sql"
            if os.path.isfile(dump_file_path):
                logger.info(f"Importing table {table} into database {db_name}.")
                with open(dump_file_path, "r") as dump_file:
                    sql_content = dump_file.read()
                    # Use sqlparse to split the SQL content into valid statements
                    statements = sqlparse.split(sql_content)
                    try:
                        # Execute each statement
                        with connection.cursor() as cursor:
                            for statement in statements:
                                statement = statement.strip()
                                if statement:  # Skip empty statements
                                    cursor.execute(statement)
                        connection.commit()
                        logger.info(
                            f"Table {table} successfully imported into {db_name}."
                        )
                    except pymysql.MySQLError as e:
                        logger.error(f"Error importing table {table}: {e}")
                        connection.rollback()
            else:
                logger.warning(f"SQL dump file for table {table} not found. Skipping.")

    except pymysql.MySQLError as e:
        logger.error(f"Error resetting and restoring the database: {e}")
        sys.exit(1)
    finally:
        if connection:
            connection.close()


def configure_logger(log_filename):
    """Create and configure a new logger instance."""
    logger = logging.getLogger(log_filename)
    logger.setLevel(logging.INFO)

    # Remove existing handlers (if any)
    if logger.handlers:
        logger.handlers.clear()

    # Create file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger


def main():
    parser = argparse.ArgumentParser(
        description="Reset the MySQL database and restore tables from SQL dump files."
    )
    parser.add_argument(
        "--database_name",
        help="Name of the database to reset and restore.",
        required=True,
    )

    args = parser.parse_args()
    # logfile path will under the current directory, date_dbname.log
    curr_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{curr_time}_{args.database_name}.log"
    logger = configure_logger(log_filename)
    reset_and_restore_database(args.database_name, logger)


if __name__ == "__main__":
    main()
