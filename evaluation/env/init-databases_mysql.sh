#!/bin/bash
set -e

# ------------------------------------------------------------------------------
# 0. Wait for MySQL to become available
# ------------------------------------------------------------------------------
echo "Waiting for MySQL to be ready..."
until mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "SELECT 1;" >/dev/null 2>&1; do
  >&2 echo "MySQL is unavailable - waiting..."
  sleep 2
done
echo "MySQL is ready!"

# ------------------------------------------------------------------------------
# 1. Adjust GLOBAL sql_mode (similar to removing ONLY_FULL_GROUP_BY, etc.)
# ------------------------------------------------------------------------------
echo "Setting GLOBAL sql_mode to skip ONLY_FULL_GROUP_BY and use other strict settings..."
mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "SET GLOBAL sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION';"

# ------------------------------------------------------------------------------
# 2. Define the list of template databases
# ------------------------------------------------------------------------------
TEMPLATE_DBS=(
  "debit_card_specializing_template"
  "financial_template"
  "formula_1_template"
  "california_schools_template"
  "card_games_template"
  "european_football_2_template"
  "thrombosis_prediction_template"
  "toxicology_template"
  "student_club_template"
  "superhero_template"
  "codebase_community_template"
)

# Path to database dump files
DUMP_PATH="/docker-entrypoint-initdb.d/mysql_table_dumps"

# ------------------------------------------------------------------------------
# 3. Function to import a database from its dump file
# ------------------------------------------------------------------------------
import_db_dump() {
  local db_name="$1"
  local dump_file="${DUMP_PATH}/${db_name}_dump.sql"
  
  echo "Importing database: ${db_name} from ${dump_file}"
  
  # Check if dump file exists
  if [[ ! -f "${dump_file}" ]]; then
    echo "  [WARN] Dump file ${dump_file} does not exist, skipping database ${db_name}"
    return 1
  fi
  
  # Create the database if it doesn't exist
  echo "  - Creating database ${db_name} (if not exists)"
  mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "CREATE DATABASE IF NOT EXISTS \`${db_name}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
  
  # Import the dump file
  echo "  - Importing dump file ${dump_file} into database ${db_name}"
  if ! mysql -u root -p"${MYSQL_ROOT_PASSWORD}" "${db_name}" < "${dump_file}" 2>>/tmp/error.log; then
    echo "    [ERROR] Failed to import ${dump_file} into ${db_name}. See /tmp/error.log"
    return 1
  fi
  
  echo "  - Successfully imported ${db_name}"
  return 0
}

# ------------------------------------------------------------------------------
# 4. Import each template database from its dump file
# ------------------------------------------------------------------------------
echo "Importing template databases from dump files..."
for DB_TEMPLATE in "${TEMPLATE_DBS[@]}"; do
  import_db_dump "${DB_TEMPLATE}"
done

# ------------------------------------------------------------------------------
# 5. Create sql_test_template database
# ------------------------------------------------------------------------------
echo "Creating sql_test_template database (if not exists)..."
mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "CREATE DATABASE IF NOT EXISTS \`sql_test_template\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# Import sql_test_template if its dump exists
if [[ -f "${DUMP_PATH}/sql_test_template_dump.sql" ]]; then
  echo "Importing sql_test_template from dump..."
  mysql -u root -p"${MYSQL_ROOT_PASSWORD}" "sql_test_template" < "${DUMP_PATH}/sql_test_template_dump.sql" 2>>/tmp/error.log || echo "  [WARN] Failed to import sql_test_template dump"
fi

# ------------------------------------------------------------------------------
# 6. Create "real" DBs by cloning from each template using mysqldump
# ------------------------------------------------------------------------------
echo "Cloning real DBs from templates using mysqldump..."
for DB_TEMPLATE in "${TEMPLATE_DBS[@]}"; do
  REAL_DB="${DB_TEMPLATE%_template}"

  DB_EXISTS=$(mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -N -B -e "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='${REAL_DB}';" || echo "0")
  if [[ "$DB_EXISTS" -eq 0 ]]; then
    echo "  -> Creating real database '${REAL_DB}' from '${DB_TEMPLATE}'"
    
    # Use mysqldump to create a dump of the template database
    echo "  -> Dumping ${DB_TEMPLATE} to temporary file"
    mysqldump -u root -p"${MYSQL_ROOT_PASSWORD}" --triggers --routines --events "${DB_TEMPLATE}" > "/tmp/${REAL_DB}_template.sql"
    
    # Create the real database
    mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "CREATE DATABASE IF NOT EXISTS \`${REAL_DB}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    
    # Import the dump into the real database
    echo "  -> Importing dump into ${REAL_DB}"
    mysql -u root -p"${MYSQL_ROOT_PASSWORD}" "${REAL_DB}" < "/tmp/${REAL_DB}_template.sql"
    
    # Verify the import
    TABLE_COUNT=$(mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${REAL_DB}' AND table_type='BASE TABLE';" || echo "0")
    echo "  -> Created ${TABLE_COUNT} tables in ${REAL_DB}"
  else
    echo "  -> Real database '${REAL_DB}' already exists. Skipping template clone."
  fi
done

# ------------------------------------------------------------------------------
# 7. Create a 'sql_test' from 'sql_test_template' using mysqldump
# ------------------------------------------------------------------------------
SQL_TEST_EXISTS=$(mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -N -B -e "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='sql_test';" || echo "0")
if [[ "$SQL_TEST_EXISTS" -eq 0 ]]; then
  echo "Creating real 'sql_test' database from 'sql_test_template'..."
  
  # Use mysqldump to create a dump of sql_test_template
  mysqldump -u root -p"${MYSQL_ROOT_PASSWORD}" --triggers --routines --events "sql_test_template" > "/tmp/sql_test_template.sql"
  
  # Create the sql_test database
  mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e "CREATE DATABASE IF NOT EXISTS \`sql_test\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
  
  # Import the dump into sql_test
  mysql -u root -p"${MYSQL_ROOT_PASSWORD}" "sql_test" < "/tmp/sql_test_template.sql"
  
  # Verify the import
  SQL_TEST_TABLE_COUNT=$(mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='sql_test' AND table_type='BASE TABLE';" || echo "0")
  echo "  -> Created ${SQL_TEST_TABLE_COUNT} tables in sql_test"
else
  echo "Database 'sql_test' already exists; skipping creation."
fi

# ------------------------------------------------------------------------------
# 8. Cleanup and final message
# ------------------------------------------------------------------------------
rm -f /tmp/error.log
rm -f /tmp/*_template.sql 2>/dev/null || true
echo "All template databases have been created, cloned to real DBs, and are ready for use!"
echo "Done."