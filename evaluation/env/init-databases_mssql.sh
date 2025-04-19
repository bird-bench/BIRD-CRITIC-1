#!/bin/bash
set -e

##############################################################################
# 0. Base environment variables & settings
##############################################################################

SA_PASSWORD="${MSSQL_SA_PASSWORD:-Y.sa123123}"
DROP_DATABASE_IF_EXISTS="${DROP_DATABASE_IF_EXISTS:-true}"
echo "DROP_DATABASE_IF_EXISTS = $DROP_DATABASE_IF_EXISTS"

# Start SQL Server in the background (container method)
/opt/mssql/bin/sqlservr &

echo "[init_from_bak.sh] Waiting for SQL Server to start..."
sleep 15

# Directory containing backup files
BACKUP_DIR="/app/mssql_table_dumps"


if [ ! -d "$BACKUP_DIR" ]; then
  echo "Warning: Directory $BACKUP_DIR does not exist or is not mounted, cannot automatically restore databases (.bak)."
  echo "If you need to restore databases from .bak files, please place them in $BACKUP_DIR inside the container."
  # Can exit directly with exit 1 or keep bcp logic as fallback
  exit 0
fi

# Get all .bak files
shopt -s nullglob
BAK_FILES=("$BACKUP_DIR"/*.bak)

if [ ${#BAK_FILES[@]} -eq 0 ]; then
  echo "Warning: No .bak files found in $BACKUP_DIR, skipping restoration."
  # Can also fallback to original bcp method here
  exit 0
fi

# Function: Drop database if it exists
drop_database_if_exists() {
  local db_name="$1"
  echo "Checking if database [$db_name] exists and dropping it if so..."

  # First attempt: If database exists, set to single user mode and force disconnect all connections
  sqlcmd \
    -S localhost \
    -No \
    -d master \
    -U SA -P "$SA_PASSWORD" \
    -Q "
      IF DB_ID('$db_name') IS NOT NULL
      BEGIN
        ALTER DATABASE [$db_name] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        PRINT 'Set to SINGLE_USER and rollback immediate.';
      END
    "

  # Second attempt: Safely drop the database (will not error if it doesn't exist)
  sqlcmd \
    -S localhost \
    -No \
    -d master \
    -U SA -P "$SA_PASSWORD" \
    -Q "
      DROP DATABASE IF EXISTS [$db_name];
      PRINT 'Dropped database if existed: $db_name';
    "
}

echo "=== Starting database restoration from .bak files ==="

for bak_file in "${BAK_FILES[@]}"; do
  # Extract database name from backup file (e.g., "debit_card_specializing_template.bak" => "debit_card_specializing")
  filename="$(basename "$bak_file")"
  db_name="${filename%_template.bak}"  # Remove '_template.bak' suffix
  
  echo ">>> Found backup file: $bak_file => database [$db_name]"

  
  # If configured to drop before creating:
  if [ "${DROP_DATABASE_IF_EXISTS,,}" = "true" ]; then
    drop_database_if_exists "$db_name"
  fi
  
  # Perform restoration
  echo ">>> Restoring database [$db_name] from $bak_file"
  sqlcmd \
    -S localhost \
    -No \
    -U SA \
    -P "$SA_PASSWORD" \
    -d master \
    -Q "
      RESTORE DATABASE [$db_name]
      FROM DISK = N'${bak_file}'
      WITH REPLACE,
           RECOVERY,
           STATS = 5;
    "
  echo "Restoration completed: database [$db_name]"
done

echo "=== All available .bak databases have been restored! ==="

# Keep container running
tail -f /dev/null