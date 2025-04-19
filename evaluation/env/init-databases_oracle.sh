#!/bin/bash

echo "=== [init-databases_oracle.sh] Create MASTER user, import schema, then load CSV ==="

: "${ORACLE_PDB:=ORCLPDB1}"
SCHEMA_SQL="/oracle_table_dumps/schema.sql"
CSV_DIR="/oracle_table_dumps/oracle_csv"
SQLLDR_LOGDIR="/oracle_table_dumps/sqlldr_logs"
SPOOL_LOG="/oracle_table_dumps/schema_import.log"


GLOBAL_ERR_LOG="$SQLLDR_LOGDIR/global_errors.log"
mkdir -p "$SQLLDR_LOGDIR"
rm -f "$GLOBAL_ERR_LOG"
touch "$GLOBAL_ERR_LOG"

# 1) Check schema.sql
if [ ! -f "$SCHEMA_SQL" ]; then
  echo "!!! $SCHEMA_SQL not found. Exiting..."
  exit 1
fi

# 2) Check CSV folder
if [ ! -d "$CSV_DIR" ]; then
  echo "!!! $CSV_DIR not found. Will only create tables from schema.sql, no CSV import."
fi

# 3) Create MASTER user in PDB
echo "=== Creating MASTER user in PDB=$ORACLE_PDB via sysdba ==="
/opt/oracle/product/19c/dbhome_1/bin/sqlplus -S / as sysdba <<EOF
ALTER PLUGGABLE DATABASE $ORACLE_PDB OPEN;
ALTER SESSION SET CONTAINER=$ORACLE_PDB;

CREATE USER MASTER IDENTIFIED BY MASTER ACCOUNT UNLOCK;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE, DBA TO MASTER;

EXIT
EOF

echo "=== MASTER user created. Now run schema.sql as MASTER to create tables. ==="

# 4) Execute schema.sql as MASTER
echo "=== Executing schema.sql => spool logs to $SPOOL_LOG ==="
/opt/oracle/product/19c/dbhome_1/bin/sqlplus -S "MASTER/MASTER@//localhost:1521/$ORACLE_PDB" <<EOF
SPOOL $SPOOL_LOG
@${SCHEMA_SQL}
SPOOL OFF
EXIT
EOF

echo "=== schema.sql done. Check $SPOOL_LOG for creation logs. ==="

# 5) If CSV folder present, load them with sqlldr MASTER/MASTER
if [ -d "$CSV_DIR" ]; then
  echo "=== Now loading CSV from $CSV_DIR => MASTER schema. Logs => $SQLLDR_LOGDIR ==="

  for csvfile in "$CSV_DIR"/*.csv; do
    [ -f "$csvfile" ] || continue
    filename=$(basename "$csvfile")
    tablename="${filename%.*}"

    echo "=== Loading $csvfile into MASTER.\"$tablename\" ==="

    CTL="/tmp/${tablename}.ctl"
    cat <<EOH > "$CTL"
OPTIONS (SKIP=1, ERRORS=999999)
LOAD DATA
INFILE '$csvfile'
INTO TABLE "MASTER"."$tablename"
APPEND
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
EOH

    # -------------------------------
    #   Big case for each table
    # -------------------------------
    case "$tablename" in

      "atom")
        cat <<EOT >> "$CTL"
  "atom_id" CHAR(4000),
  "molecule_id" CHAR(4000),
  "element" CHAR(4000)
)
EOT
        ;;

      "bond")
        cat <<EOT >> "$CTL"
  "bond_id" CHAR(4000),
  "molecule_id" CHAR(4000),
  "bond_type" CHAR(4000)
)
EOT
        ;;

      "connected")
        cat <<EOT >> "$CTL"
  "atom_id" CHAR(4000),
  "atom_id2" CHAR(4000),
  "bond_id" CHAR(4000)
)
EOT
        ;;

      "molecule")
        cat <<EOT >> "$CTL"
  "molecule_id" CHAR(4000),
  "label" CHAR(4000)
)
EOT
        ;;

      "alignment")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "alignment" CHAR(4000)
)
EOT
        ;;

      "attribute")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "attribute_name" CHAR(4000)
)
EOT
        ;;

      "colour")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "colour" CHAR(4000)
)
EOT
        ;;

      "gender")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "gender" CHAR(4000)
)
EOT
        ;;

      "hero_attribute")
        cat <<EOT >> "$CTL"
  "hero_id" INTEGER EXTERNAL,
  "attribute_id" INTEGER EXTERNAL,
  "attribute_value" INTEGER EXTERNAL
)
EOT
        ;;

      "hero_power")
        cat <<EOT >> "$CTL"
  "hero_id" INTEGER EXTERNAL,
  "power_id" INTEGER EXTERNAL
)
EOT
        ;;

      "publisher")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "publisher_name" CHAR(4000)
)
EOT
        ;;

      "race")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "race" CHAR(4000)
)
EOT
        ;;

      "superhero")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "superhero_name" CHAR(4000),
  "full_name" CHAR(4000),
  "gender_id" INTEGER EXTERNAL,
  "eye_colour_id" INTEGER EXTERNAL,
  "hair_colour_id" INTEGER EXTERNAL,
  "skin_colour_id" INTEGER EXTERNAL,
  "race_id" INTEGER EXTERNAL,
  "publisher_id" INTEGER EXTERNAL,
  "alignment_id" INTEGER EXTERNAL,
  "height_cm" INTEGER EXTERNAL,
  "weight_kg" INTEGER EXTERNAL
)
EOT
        ;;

      "superpower")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "power_name" CHAR(4000)
)
EOT
        ;;

      "attendance")
        cat <<EOT >> "$CTL"
  "link_to_event" CHAR(4000),
  "link_to_member" CHAR(4000)
)
EOT
        ;;

      "budget")
        cat <<EOT >> "$CTL"
  "budget_id" CHAR(4000),
  "category" CHAR(4000),
  "spent" DECIMAL EXTERNAL,
  "remaining" DECIMAL EXTERNAL,
  "amount" INTEGER EXTERNAL,
  "event_status" CHAR(4000),
  "link_to_event" CHAR(4000)
)
EOT
        ;;

      "event")
        cat <<EOT >> "$CTL"
  "event_id" CHAR(4000),
  "event_name" CHAR(4000),
  "event_date" DATE "YYYY-MM-DD\"T\"HH24:MI:SS",
  "type" CHAR(4000),
  "notes" CHAR(4000),
  "location" CHAR(4000),
  "status" CHAR(4000)
)
EOT
        ;;

      "expense")
        cat <<EOT >> "$CTL"
  "expense_id" CHAR(4000),
  "expense_description" CHAR(4000),
  "expense_date" DATE "YYYY-MM-DD",
  "cost" INTEGER EXTERNAL,
  "approved" CHAR(4000),
  "link_to_member" CHAR(4000),
  "link_to_budget" CHAR(4000)
)
EOT
        ;;

      "income")
        cat <<EOT >> "$CTL"
  "income_id" CHAR(4000),
  "date_received" DATE "YYYY-MM-DD",
  "amount" INTEGER EXTERNAL,
  "source" CHAR(4000),
  "notes" CHAR(4000),
  "link_to_member" CHAR(4000)
)
EOT
        ;;

      "major")
        cat <<EOT >> "$CTL"
  "major_id" CHAR(4000),
  "major_name" CHAR(4000),
  "department" CHAR(4000),
  "college" CHAR(4000)
)
EOT
        ;;

      "member")
        cat <<EOT >> "$CTL"
  "member_id" CHAR(4000),
  "first_name" CHAR(4000),
  "last_name" CHAR(4000),
  "email" CHAR(4000),
  "position" CHAR(4000),
  "t_shirt_size" CHAR(4000),
  "phone" CHAR(4000),
  "zip" INTEGER EXTERNAL,
  "link_to_major" CHAR(4000)
)
EOT
        ;;

      "zip_code")
        cat <<EOT >> "$CTL"
  "zip_code" INTEGER EXTERNAL,
  "type" CHAR(4000),
  "city" CHAR(4000),
  "county" CHAR(4000),
  "state" CHAR(4000),
  "short_state" CHAR(4000)
)
EOT
        ;;

      "Country")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "name" CHAR(4000)
)
EOT
        ;;

      "League")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "country_id" INTEGER EXTERNAL,
  "name" CHAR(4000)
)
EOT
        ;;

      "Match")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "country_id" INTEGER EXTERNAL,
  "league_id" INTEGER EXTERNAL,
  "season" CHAR(4000),
  "stage" INTEGER EXTERNAL,
  "date" DATE "YYYY-MM-DD HH24:MI:SS",
  "match_api_id" INTEGER EXTERNAL,
  "home_team_api_id" INTEGER EXTERNAL,
  "away_team_api_id" INTEGER EXTERNAL,
  "home_team_goal" INTEGER EXTERNAL,
  "away_team_goal" INTEGER EXTERNAL,
  "home_player_X1" INTEGER EXTERNAL,
  "home_player_X2" INTEGER EXTERNAL,
  "home_player_X3" INTEGER EXTERNAL,
  "home_player_X4" INTEGER EXTERNAL,
  "home_player_X5" INTEGER EXTERNAL,
  "home_player_X6" INTEGER EXTERNAL,
  "home_player_X7" INTEGER EXTERNAL,
  "home_player_X8" INTEGER EXTERNAL,
  "home_player_X9" INTEGER EXTERNAL,
  "home_player_X10" INTEGER EXTERNAL,
  "home_player_X11" INTEGER EXTERNAL,
  "away_player_X1" INTEGER EXTERNAL,
  "away_player_X2" INTEGER EXTERNAL,
  "away_player_X3" INTEGER EXTERNAL,
  "away_player_X4" INTEGER EXTERNAL,
  "away_player_X5" INTEGER EXTERNAL,
  "away_player_X6" INTEGER EXTERNAL,
  "away_player_X7" INTEGER EXTERNAL,
  "away_player_X8" INTEGER EXTERNAL,
  "away_player_X9" INTEGER EXTERNAL,
  "away_player_X10" INTEGER EXTERNAL,
  "away_player_X11" INTEGER EXTERNAL,
  "home_player_Y1" INTEGER EXTERNAL,
  "home_player_Y2" INTEGER EXTERNAL,
  "home_player_Y3" INTEGER EXTERNAL,
  "home_player_Y4" INTEGER EXTERNAL,
  "home_player_Y5" INTEGER EXTERNAL,
  "home_player_Y6" INTEGER EXTERNAL,
  "home_player_Y7" INTEGER EXTERNAL,
  "home_player_Y8" INTEGER EXTERNAL,
  "home_player_Y9" INTEGER EXTERNAL,
  "home_player_Y10" INTEGER EXTERNAL,
  "home_player_Y11" INTEGER EXTERNAL,
  "away_player_Y1" INTEGER EXTERNAL,
  "away_player_Y2" INTEGER EXTERNAL,
  "away_player_Y3" INTEGER EXTERNAL,
  "away_player_Y4" INTEGER EXTERNAL,
  "away_player_Y5" INTEGER EXTERNAL,
  "away_player_Y6" INTEGER EXTERNAL,
  "away_player_Y7" INTEGER EXTERNAL,
  "away_player_Y8" INTEGER EXTERNAL,
  "away_player_Y9" INTEGER EXTERNAL,
  "away_player_Y10" INTEGER EXTERNAL,
  "away_player_Y11" INTEGER EXTERNAL,
  "home_player_1" INTEGER EXTERNAL,
  "home_player_2" INTEGER EXTERNAL,
  "home_player_3" INTEGER EXTERNAL,
  "home_player_4" INTEGER EXTERNAL,
  "home_player_5" INTEGER EXTERNAL,
  "home_player_6" INTEGER EXTERNAL,
  "home_player_7" INTEGER EXTERNAL,
  "home_player_8" INTEGER EXTERNAL,
  "home_player_9" INTEGER EXTERNAL,
  "home_player_10" INTEGER EXTERNAL,
  "home_player_11" INTEGER EXTERNAL,
  "away_player_1" INTEGER EXTERNAL,
  "away_player_2" INTEGER EXTERNAL,
  "away_player_3" INTEGER EXTERNAL,
  "away_player_4" INTEGER EXTERNAL,
  "away_player_5" INTEGER EXTERNAL,
  "away_player_6" INTEGER EXTERNAL,
  "away_player_7" INTEGER EXTERNAL,
  "away_player_8" INTEGER EXTERNAL,
  "away_player_9" INTEGER EXTERNAL,
  "away_player_10" INTEGER EXTERNAL,
  "away_player_11" INTEGER EXTERNAL,
  "goal" CHAR(4000),
  "shoton" CHAR(4000),
  "shotoff" CHAR(4000),
  "foulcommit" CHAR(4000),
  "card" CHAR(4000),
  "cross" CHAR(4000),
  "corner" CHAR(4000),
  "possession" CHAR(4000),
  B365H DECIMAL EXTERNAL,
  B365D DECIMAL EXTERNAL,
  B365A DECIMAL EXTERNAL,
  BWH DECIMAL EXTERNAL,
  BWD DECIMAL EXTERNAL,
  BWA DECIMAL EXTERNAL,
  IWH DECIMAL EXTERNAL,
  IWD DECIMAL EXTERNAL,
  IWA DECIMAL EXTERNAL,
  LBH DECIMAL EXTERNAL,
  LBD DECIMAL EXTERNAL,
  LBA DECIMAL EXTERNAL,
  PSH DECIMAL EXTERNAL,
  PSD DECIMAL EXTERNAL,
  PSA DECIMAL EXTERNAL,
  WHH DECIMAL EXTERNAL,
  WHD DECIMAL EXTERNAL,
  WHA DECIMAL EXTERNAL,
  SJH DECIMAL EXTERNAL,
  SJD DECIMAL EXTERNAL,
  SJA DECIMAL EXTERNAL,
  VCH DECIMAL EXTERNAL,
  VCD DECIMAL EXTERNAL,
  VCA DECIMAL EXTERNAL,
  GBH DECIMAL EXTERNAL,
  GBD DECIMAL EXTERNAL,
  GBA DECIMAL EXTERNAL,
  BSH DECIMAL EXTERNAL,
  BSD DECIMAL EXTERNAL,
  BSA DECIMAL EXTERNAL
)
EOT
        ;;

      "Player")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "player_api_id" INTEGER EXTERNAL,
  "player_name" CHAR(4000),
  "player_fifa_api_id" INTEGER EXTERNAL,
  "birthday" DATE "YYYY-MM-DD HH24:MI:SS",
  "height" INTEGER EXTERNAL,
  "weight" INTEGER EXTERNAL
)
EOT
        ;;

      "Player_Attributes")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "player_fifa_api_id" INTEGER EXTERNAL,
  "player_api_id" INTEGER EXTERNAL,
  "date" DATE "YYYY-MM-DD HH24:MI:SS",
  "overall_rating" INTEGER EXTERNAL,
  "potential" INTEGER EXTERNAL,
  "preferred_foot" CHAR(4000),
  "attacking_work_rate" CHAR(4000),
  "defensive_work_rate" CHAR(4000),
  "crossing" INTEGER EXTERNAL,
  "finishing" INTEGER EXTERNAL,
  "heading_accuracy" INTEGER EXTERNAL,
  "short_passing" INTEGER EXTERNAL,
  "volleys" INTEGER EXTERNAL,
  "dribbling" INTEGER EXTERNAL,
  "curve" INTEGER EXTERNAL,
  "free_kick_accuracy" INTEGER EXTERNAL,
  "long_passing" INTEGER EXTERNAL,
  "ball_control" INTEGER EXTERNAL,
  "acceleration" INTEGER EXTERNAL,
  "sprint_speed" INTEGER EXTERNAL,
  "agility" INTEGER EXTERNAL,
  "reactions" INTEGER EXTERNAL,
  "balance" INTEGER EXTERNAL,
  "shot_power" INTEGER EXTERNAL,
  "jumping" INTEGER EXTERNAL,
  "stamina" INTEGER EXTERNAL,
  "strength" INTEGER EXTERNAL,
  "long_shots" INTEGER EXTERNAL,
  "aggression" INTEGER EXTERNAL,
  "interceptions" INTEGER EXTERNAL,
  "positioning" INTEGER EXTERNAL,
  "vision" INTEGER EXTERNAL,
  "penalties" INTEGER EXTERNAL,
  "marking" INTEGER EXTERNAL,
  "standing_tackle" INTEGER EXTERNAL,
  "sliding_tackle" INTEGER EXTERNAL,
  "gk_diving" INTEGER EXTERNAL,
  "gk_handling" INTEGER EXTERNAL,
  "gk_kicking" INTEGER EXTERNAL,
  "gk_positioning" INTEGER EXTERNAL,
  "gk_reflexes" INTEGER EXTERNAL
)
EOT
        ;;

      "Team")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "team_api_id" INTEGER EXTERNAL,
  "team_fifa_api_id" INTEGER EXTERNAL,
  "team_long_name" CHAR(4000),
  "team_short_name" CHAR(4000)
)
EOT
        ;;

      "Team_Attributes")
        cat <<EOT >> "$CTL"
  "id" INTEGER EXTERNAL,
  "team_fifa_api_id" INTEGER EXTERNAL,
  "team_api_id" INTEGER EXTERNAL,
  "date" DATE "YYYY-MM-DD HH24:MI:SS",
  "buildUpPlaySpeed" INTEGER EXTERNAL,
  "buildUpPlaySpeedClass" CHAR(4000),
  "buildUpPlayDribbling" INTEGER EXTERNAL,
  "buildUpPlayDribblingClass" CHAR(4000),
  "buildUpPlayPassing" INTEGER EXTERNAL,
  "buildUpPlayPassingClass" CHAR(4000),
  "buildUpPlayPositioningClass" CHAR(4000),
  "chanceCreationPassing" INTEGER EXTERNAL,
  "chanceCreationPassingClass" CHAR(4000),
  "chanceCreationCrossing" INTEGER EXTERNAL,
  "chanceCreationCrossingClass" CHAR(4000),
  "chanceCreationShooting" INTEGER EXTERNAL,
  "chanceCreationShootingClass" CHAR(4000),
  "chanceCreationPositioningClass" CHAR(4000),
  "defencePressure" INTEGER EXTERNAL,
  "defencePressureClass" CHAR(4000),
  "defenceAggression" INTEGER EXTERNAL,
  "defenceAggressionClass" CHAR(4000),
  "defenceTeamWidth" INTEGER EXTERNAL,
  "defenceTeamWidthClass" CHAR(4000),
  "defenceDefenderLineClass" CHAR(4000)
)
EOT
        ;;

      "schools")
        cat <<EOT >> "$CTL"
  "CDSCode" CHAR(4000),
  "NCESDist" CHAR(4000),
  "NCESSchool" CHAR(4000),
  "StatusType" CHAR(4000),
  "County" CHAR(4000),
  "District" CHAR(4000),
  "School" CHAR(4000),
  "Street" CHAR(4000),
  "StreetAbr" CHAR(4000),
  "City" CHAR(4000),
  "Zip" CHAR(4000),
  "State" CHAR(4000),
  "MailStreet" CHAR(4000),
  "MailStrAbr" CHAR(4000),
  "MailCity" CHAR(4000),
  "MailZip" CHAR(4000),
  "MailState" CHAR(4000),
  "Phone" CHAR(4000),
  "Ext" CHAR(4000),
  "Website" CHAR(4000),
  "OpenDate" DATE "YYYY-MM-DD",
  "ClosedDate" DATE "YYYY-MM-DD",
  "Charter" INTEGER EXTERNAL,
  "CharterNum" CHAR(4000),
  "FundingType" CHAR(4000),
  DOC CHAR(4000),
  "DOCType" CHAR(4000),
  SOC CHAR(4000),
  "SOCType" CHAR(4000),
  "EdOpsCode" CHAR(4000),
  "EdOpsName" CHAR(4000),
  "EILCode" CHAR(4000),
  "EILName" CHAR(4000),
  "GSoffered" CHAR(4000),
  "GSserved" CHAR(4000),
  "Virtual" CHAR(4000),
  "Magnet" INTEGER EXTERNAL,
  "Latitude" DECIMAL EXTERNAL,
  "Longitude" DECIMAL EXTERNAL,
  "AdmFName1" CHAR(4000),
  "AdmLName1" CHAR(4000),
  "AdmEmail1" CHAR(4000),
  "AdmFName2" CHAR(4000),
  "AdmLName2" CHAR(4000),
  "AdmEmail2" CHAR(4000),
  "AdmFName3" CHAR(4000),
  "AdmLName3" CHAR(4000),
  "AdmEmail3" CHAR(4000),
  "LastUpdate" DATE "YYYY-MM-DD"
)
EOT
        ;;

      "frpm")
        cat <<EOT >> "$CTL"
  "CDSCode" CHAR(4000),
  "Academic Year" CHAR(4000),
  "County Code" CHAR(4000),
  "District Code" INTEGER EXTERNAL,
  "School Code" CHAR(4000),
  "County Name" CHAR(4000),
  "District Name" CHAR(4000),
  "School Name" CHAR(4000),
  "District Type" CHAR(4000),
  "School Type" CHAR(4000),
  "Educational Option Type" CHAR(4000),
  "NSLP Provision Status" CHAR(4000),
  "Charter School (Y/N)" INTEGER EXTERNAL,
  "Charter School Number" CHAR(4000),
  "Charter Funding Type" CHAR(4000),
  IRC INTEGER EXTERNAL,
  "Low Grade" CHAR(4000),
  "High Grade" CHAR(4000),
  "Enrollment (K-12)" DECIMAL EXTERNAL,
  "Free Meal Count (K-12)" DECIMAL EXTERNAL,
  "Percent (%) Eligible Free (K-12)" DECIMAL EXTERNAL,
  "FRPM Count (K-12)" DECIMAL EXTERNAL,
  "Percent (%) Eligible FRPM (K-12)" DECIMAL EXTERNAL,
  "Enrollment (Ages 5-17)" DECIMAL EXTERNAL,
  "Free Meal Count (Ages 5-17)" DECIMAL EXTERNAL,
  "Percent (%) Eligible Free (Ages 5-17)" DECIMAL EXTERNAL,
  "FRPM Count (Ages 5-17)" DECIMAL EXTERNAL,
  "Percent (%) Eligible FRPM (Ages 5-17)" DECIMAL EXTERNAL,
  "2013-14 CALPADS Fall 1 Certification Status" INTEGER EXTERNAL
)
EOT
        ;;

      "satscores")
        cat <<EOT >> "$CTL"
  "cds" CHAR(4000),
  "rtype" CHAR(4000),
  "sname" CHAR(4000),
  "dname" CHAR(4000),
  "cname" CHAR(4000),
  "enroll12" INTEGER EXTERNAL,
  "NumTstTakr" INTEGER EXTERNAL,
  "AvgScrRead" INTEGER EXTERNAL,
  "AvgScrMath" INTEGER EXTERNAL,
  "AvgScrWrite" INTEGER EXTERNAL,
  "NumGE1500" INTEGER EXTERNAL
)
EOT
        ;;

      # default => no columns => fail
      *)
        cat <<EOT >> "$CTL"
  -- ??? no columns ??? => Will fail
)
EOT
        echo "!!! Table $tablename not recognized => .ctl empty => Will fail."
        ;;
    esac

    # use sqlldr to load the CSV into the table
    /opt/oracle/product/19c/dbhome_1/bin/sqlldr MASTER/MASTER@//localhost:1521/$ORACLE_PDB \
      CONTROL="$CTL" \
      LOG="$SQLLDR_LOGDIR/${tablename}.log" \
      BAD="$SQLLDR_LOGDIR/${tablename}.bad" \
      DIRECT=TRUE

    exit_code=$?
    if [ $exit_code -ne 0 ]; then
      echo "[ERROR] sqlldr for table $tablename failed with code $exit_code. Check ${tablename}.log" >> "$GLOBAL_ERR_LOG"
      # Does not exit, continue to next table
    else
      echo "[OK] sqlldr for table $tablename succeeded."
    fi

  done
fi

echo "=== All done with CSV loading ==="

# 6) If any errors were encountered, print them
if [ -s "$GLOBAL_ERR_LOG" ]; then
  echo "=== The following errors were encountered during CSV loading ==="
  cat "$GLOBAL_ERR_LOG"
  echo "=== End of error log ==="
else
  echo "=== No major sqlldr errors encountered. ==="
fi