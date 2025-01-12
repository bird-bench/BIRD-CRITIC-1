#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until psql -U root -c '\l'; do
  >&2 echo "PostgreSQL is unavailable - waiting..."
  sleep 2
done

echo "PostgreSQL is ready!"

# Step 0: Create the empty test database 'sql_test'
echo "Creating empty test database: sql_test"
psql -U root -c "CREATE DATABASE sql_test;" || echo "Test database sql_test already exists."

# Define the database mappings
declare -A DATABASE_MAPPING=(
    ["debit_card_specializing"]="customers gasstations products yearmonth transactions_1k"
    ["financial"]="loan client district trans account card order disp"
    ["formula_1"]="circuits status drivers driverstandings races constructors constructorresults laptimes qualifying pitstops seasons constructorstandings results"
    ["california_schools"]="schools satscores frpm"
    ["card_games"]="legalities cards rulings set_translations sets foreign_data"
    ["european_football_2"]="team_attributes player match league country player_attributes team"
    ["thrombosis_prediction"]="laboratory patient examination"
    ["toxicology"]="bond molecule atom connected"
    ["student_club"]="income budget zip_code expense member attendance event major"
    ["superhero"]="gender superpower publisher superhero colour attribute hero_power race alignment hero_attribute"
    ["codebase_community"]="postlinks posthistory badges posts users tags votes comments"
)

# Lowercased ordered list of tables based on dependency analysis
TABLE_ORDER=(
    hero_power votes disp foreign_data attendance loan myperms postlinks superpower match molecule
    qualifying badges yearmonth connected event my_table set_translations rulings expense card
    laptimes posthistory cards results hero_attribute legalities tags player_attributes laboratory
    member status products proccd trans zip_code seasons schools team_attributes sets pitstops
    satscores examination transactions_1k order patient district comments superhero frpm income
    gasstations constructorstandings constructorresults league driverstandings users posts client customers
    atom bond budget races attribute player major team account race publisher gender alignment colour
    constructors country drivers circuits
)

# Step 1: Create each database
for DB in "${!DATABASE_MAPPING[@]}"; do
    echo "Creating database: $DB"
    psql -U root -c "CREATE DATABASE $DB;" || echo "Database $DB already exists."
done

# Function to import a table and log errors, ignoring case for file lookup
import_table() {
    local table_name_lower=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    local db_lower=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    local table_file=$(find /docker-entrypoint-initdb.d/postgre_table_dumps -iname "${table_name_lower}.sql")

    echo "Importing table '$1' into database '$2'"

    if [[ -f "$table_file" ]]; then
        if ! psql -U root -d "$db_lower" -f "$table_file" 2>>/tmp/error.log; then
            echo "Error importing table '$1' into database '$2'. Check /tmp/error.log for details."
        fi
    else
        echo "Warning: Table file for '$1' not found in /docker-entrypoint-initdb.d/postgre_table_dumps, skipping."
    fi
}

# Step 2: Import tables in the specified order
for table_name in "${TABLE_ORDER[@]}"; do
    for db in "${!DATABASE_MAPPING[@]}"; do
        if [[ " ${DATABASE_MAPPING[$db]} " =~ " ${table_name} " ]]; then
            import_table "$table_name" "$db"
            break
        fi
    done
done

# Check for errors
if [[ -s /tmp/error.log ]]; then
    echo "Errors occurred during import:"
    cat /tmp/error.log
fi

# Clean up error log
rm -f /tmp/error.log