#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until psql -U root -c '\l' 2>/dev/null; do
  >&2 echo "PostgreSQL is unavailable - waiting..."
  sleep 2
done

echo "PostgreSQL is ready!"

############################
# 0. 创建一个作为“测试模板”的数据库 sql_test_template
############################
echo "Creating template database: sql_test_template with UTF8 encoding (formerly sql_test)"
psql -U root -tc "SELECT 1 FROM pg_database WHERE datname='sql_test_template'" | grep -q 1 \
  || psql -U root -c "CREATE DATABASE sql_test_template WITH OWNER=root ENCODING='UTF8' TEMPLATE=template0;"

# 在 sql_test_template 库中创建 schema
echo "Creating required schemas: test_schema and test_schema_2 in sql_test_template"
psql -U root -d sql_test_template -c "CREATE SCHEMA IF NOT EXISTS test_schema;"
psql -U root -d sql_test_template -c "CREATE SCHEMA IF NOT EXISTS test_schema_2;"

# 创建 hstore、citext 扩展
echo "Creating hstore and citext extensions in sql_test_template..."
psql -U root -d sql_test_template -c "CREATE EXTENSION IF NOT EXISTS hstore;"
psql -U root -d sql_test_template -c "CREATE EXTENSION IF NOT EXISTS citext;"

# 设置默认全文搜索配置
echo "Setting default_text_search_config to pg_catalog.english in sql_test_template..."
psql -U root -d sql_test_template -c "ALTER DATABASE sql_test_template SET default_text_search_config = 'pg_catalog.english';"

echo "NOTE: For two-phase transaction support, set 'max_prepared_transactions' > 0 in postgresql.conf."

############################
# 1. 定义数据库→表清单，并将每个库改成“xxx_template”命名
############################
declare -A DATABASE_MAPPING=(
    ["debit_card_specializing_template"]="customers gasstations products yearmonth transactions_1k"
    ["financial_template"]="loan client district trans account card order disp"
    ["formula_1_template"]="circuits status drivers driverstandings races constructors constructorresults laptimes qualifying pitstops seasons constructorstandings results"
    ["california_schools_template"]="schools satscores frpm"
    ["card_games_template"]="legalities cards rulings set_translations sets foreign_data"
    ["european_football_2_template"]="team_attributes player match league country player_attributes team"
    ["thrombosis_prediction_template"]="laboratory patient examination"
    ["toxicology_template"]="bond molecule atom connected"
    ["student_club_template"]="income budget zip_code expense member attendance event major"
    ["superhero_template"]="gender superpower publisher superhero colour attribute hero_power race alignment hero_attribute"
    ["codebase_community_template"]="postlinks posthistory badges posts users tags votes comments"
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

############################
# 2. 创建各个“模板数据库”，并导入表数据
############################
for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
    echo "Creating template database: $DB_TEMPLATE"
    psql -U root -tc "SELECT 1 FROM pg_database WHERE datname='${DB_TEMPLATE}'" | grep -q 1 \
      || psql -U root -c "CREATE DATABASE ${DB_TEMPLATE} WITH OWNER=root ENCODING='UTF8' TEMPLATE=template0;"
done

# 函数：导入某个table到指定数据库
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

# 按 TABLE_ORDER 顺序给每个 template 数据库导入对应的表
for table_name in "${TABLE_ORDER[@]}"; do
    for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
        if [[ " ${DATABASE_MAPPING[$DB_TEMPLATE]} " =~ " ${table_name} " ]]; then
            import_table "$table_name" "$DB_TEMPLATE"
            break
        fi
    done
done

# 检查是否有导入错误
if [[ -s /tmp/error.log ]]; then
    echo "Errors occurred during import:"
    cat /tmp/error.log
fi

rm -f /tmp/error.log

############################
# 3. 可选：将这些“xxx_template”设置为真正的“template数据库” (需要superuser)
############################
echo "Marking these template databases as 'datistemplate = true' so they can be used as official templates..."
for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
  # 一般需要 postgres 超级用户权限，也可以在 docker-compose.yml 中设定 PGUSER=postgres PGPASSWORD=xxx
  psql -U root -d postgres -c "UPDATE pg_database SET datistemplate = true WHERE datname = '${DB_TEMPLATE}';" || true
done

############################
# 使用示例
############################
echo "All template databases created. To reset a DB from template, for example 'financial':"
echo "    dropdb financial || true"
echo "    createdb financial --template=financial_template"
echo ""
echo "Done."

echo "Now creating a real DB from each template DB..."

for DB_TEMPLATE in "${!DATABASE_MAPPING[@]}"; do
  # real DB 名称 = 去掉 "_template" 后缀
  REAL_DB="${DB_TEMPLATE%_template}"

  echo "Checking if real database '${REAL_DB}' exists..."
  # 如果真正的数据库还没创建，则基于模板库创建它
  EXISTS=$(psql -U root -tc "SELECT 1 FROM pg_database WHERE datname='${REAL_DB}'" | grep -c 1 || true)
  if [[ "$EXISTS" -eq 0 ]]; then
    echo "Creating real database '${REAL_DB}' from template '${DB_TEMPLATE}'"
    psql -U root -c "CREATE DATABASE ${REAL_DB} WITH OWNER=root TEMPLATE=${DB_TEMPLATE};"
  else
    echo "Database '${REAL_DB}' already exists, skipping creation."
  fi
done

echo "Done creating real DBs."



echo "ADD new database:"
psql -U root -c "CREATE DATABASE XXXX WITH OWNER=root;"