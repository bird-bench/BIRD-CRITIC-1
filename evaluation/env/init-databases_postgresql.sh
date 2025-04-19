#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until psql -U root -c '\l' 2>/dev/null; do
  >&2 echo "PostgreSQL is unavailable - waiting..."
  sleep 2
done

echo "PostgreSQL is ready!"

############################
# 0. Create template DB: sql_test_template
############################
echo "Creating template database: sql_test_template with UTF8 encoding (formerly sql_test)"
psql -U root -tc "SELECT 1 FROM pg_database WHERE datname='sql_test_template'" | grep -q 1 \
  || psql -U root -c "CREATE DATABASE sql_test_template WITH OWNER=root ENCODING='UTF8' TEMPLATE=template0;"

# Create schemas in sql_test_template
echo "Creating required schemas: test_schema and test_schema_2 in sql_test_template"
psql -U root -d sql_test_template -c "CREATE SCHEMA IF NOT EXISTS test_schema;"
psql -U root -d sql_test_template -c "CREATE SCHEMA IF NOT EXISTS test_schema_2;"

# Create hstore, citext extensions
echo "Creating hstore and citext extensions in sql_test_template..."
psql -U root -d sql_test_template -c "CREATE EXTENSION IF NOT EXISTS hstore;"
psql -U root -d sql_test_template -c "CREATE EXTENSION IF NOT EXISTS citext;"

# Set default_text_search_config
echo "Setting default_text_search_config to pg_catalog.english in sql_test_template..."
psql -U root -d sql_test_template -c "ALTER DATABASE sql_test_template SET default_text_search_config = 'pg_catalog.english';"

echo "NOTE: For two-phase transaction support, set 'max_prepared_transactions' > 0 in postgresql.conf."

############################
# 1. Define DB → tables mapping
############################
declare -A DATABASE_MAPPING=(
    ["debit_card_specializing_template"]="customers gasstations products yearmonth transactions_1k"
    ["financial_template"]="district client account disp loan trans card order"
    ["formula_1_template"]="circuits status drivers driverstandings seasons constructors races constructorresults laptimes qualifying pitstops constructorstandings results"
    ["california_schools_template"]="schools satscores frpm"
    ["card_games_template"]="legalities cards rulings sets set_translations foreign_data"
    ["european_football_2_template"]="team league country player team_attributes player_attributes match"
    ["thrombosis_prediction_template"]="patient laboratory examination"
    ["toxicology_template"]="molecule bond atom connected"
    ["student_club_template"]="zip_code event major budget member income expense attendance"
    ["superhero_template"]="gender alignment colour attribute publisher superpower race superhero hero_power hero_attribute"
    ["codebase_community_template"]="tags postlinks users posthistory badges comments posts votes"
    ["erolp_template"]="learners institutions curricula educational_resources external_factors outcomes enrollment curriculum_resources resource_usage performance_predictions target_labels"
    ["esophageal_template"]="patients demographics lifestyle_and_risk_factors patient_addresses clinical_status icd_classifications patient_icd_codes staging_systems patient_staging pathology_and_surgery treatment_and_followup"
    ["global_atlas_template"]="country city province economy population politics religion ethnicgroup spoken language countrypops countryothername countrylocalname provpops provinceothername provincelocalname citypops cityothername citylocalname continent borders encompasses organization ismember"
    ["spotify_template"]="sp_artists sp_albums sp_album_ctb sp_tracks sp_album_tracks sp_track_artists sp_track_info sp_audio_feats"
)

############################
# 2. Create template DBs and import data
############################
for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
    echo "Creating template database: $DB_TEMPLATE"
    psql -U root -tc "SELECT 1 FROM pg_database WHERE datname='${DB_TEMPLATE}'" | grep -q 1 \
      || psql -U root -c "CREATE DATABASE ${DB_TEMPLATE} WITH OWNER=root ENCODING='UTF8' TEMPLATE=template0;"
done

# Function to import files from database-specific folders
import_db_files() {
    local db_template="$1"
    local db_folder="/docker-entrypoint-initdb.d/postgre_table_dumps/${db_template}"
    
    echo "Importing files for ${db_template} from ${db_folder}"
    
    # Check if the folder exists
    if [[ ! -d "${db_folder}" ]]; then
        echo "Warning: Folder ${db_folder} does not exist, skipping database ${db_template}"
        return
    fi
    
    # Special case for global_atlas_template
    if [[ "${db_template}" == "global_atlas_template" ]]; then
        # Check if the schema and inputs files exist
        local schema_file="${db_folder}/global_atlas-schema.sql"
        local inputs_file="${db_folder}/global_atlas-inputs.sql"
        
        if [[ -f "${schema_file}" && -f "${inputs_file}" ]]; then
            echo "Importing global_atlas schema file to ${db_template}..."
            psql -U root -d "${db_template}" -f "${schema_file}" 2>>/tmp/error.log \
                || echo "Error importing schema file for ${db_template}. Check /tmp/error.log for details."
            
            echo "Importing global_atlas data file to ${db_template}..."
            psql -U root -d "${db_template}" -f "${inputs_file}" 2>>/tmp/error.log \
                || echo "Error importing data file for ${db_template}. Check /tmp/error.log for details."
        else
            # If the special files don't exist, fall back to importing individual table files
            echo "Special global_atlas files not found, falling back to individual table imports."
            import_table_files "${db_template}" "${db_folder}"
        fi
    else
        # Regular case: import all table files in the folder
        import_table_files "${db_template}" "${db_folder}"
    fi
}

# Function to import individual table files
import_table_files() {
    local db_template="$1"
    local db_folder="$2"
    local tables="${DATABASE_MAPPING[$db_template]}"
    
    for table in $tables; do
        local sql_file="${db_folder}/${table}.sql"
        if [[ -f "$sql_file" ]]; then
            echo "Importing ${sql_file} into database ${db_template}"
            if ! psql -U root -d "${db_template}" -f "${sql_file}" 2>>/tmp/error.log; then
                echo "Error importing ${sql_file} into database ${db_template}. Check /tmp/error.log for details."
            fi
        else
            echo "Warning: SQL file ${sql_file} not found for table ${table}"
        fi
    done
}

# Import data for each database
for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
    import_db_files "${DB_TEMPLATE}"
done

if [[ -s /tmp/error.log ]]; then
    echo "Errors occurred during import:"
    cat /tmp/error.log
fi

# rm -f /tmp/error.log

############################
# 3. Mark these template DBs as 'datistemplate = true'
############################
echo "Marking these template databases as 'datistemplate = true'..."
for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
  psql -U root -d postgres -c "UPDATE pg_database SET datistemplate = true WHERE datname = '${DB_TEMPLATE}';" || true
done

############################
# Example usage
############################
echo "All template databases created. For example, to clone 'financial_template' into 'financial':"
echo "    dropdb financial || true"
echo "    createdb financial --template=financial_template"
echo ""
echo "Done creating template DBs."

echo "Now creating real DB from each template DB..."

for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
  REAL_DB="${DB_TEMPLATE%_template}"
  echo "Checking if real database '${REAL_DB}' exists..."
  EXISTS=$(psql -U root -tc "SELECT 1 FROM pg_database WHERE datname='${REAL_DB}'" | grep -c 1 || true)
  if [[ "$EXISTS" -eq 0 ]]; then
    echo "Creating real database '${REAL_DB}' from template '${DB_TEMPLATE}'"
    psql -U root -c "CREATE DATABASE ${REAL_DB} WITH OWNER=root TEMPLATE=${DB_TEMPLATE};"
  else
    echo "Database '${REAL_DB}' already exists, skipping creation."
  fi
done

echo "Done creating real DBs."