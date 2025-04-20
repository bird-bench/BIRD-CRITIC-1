mysql_file="/app/data/mysql_100.jsonl" 
mssql_file="/app/data/mssql_100.jsonl"
oracle_file="/app/data/oracle_100.jsonl"
postgresql_file="/app/data/postgresql_300.jsonl"

dialect="mssql" # "mysql", "mssql", "oracle", "postgresql"

if [ "$dialect" == "mysql" ]; then
    jsonl_file="$mysql_file"
elif [ "$dialect" == "mssql" ]; then
    jsonl_file="$mssql_file"
elif [ "$dialect" == "oracle" ]; then
    jsonl_file="$oracle_file"
else
    jsonl_file="$postgresql_file"
fi
# "pred" for prediction, "gold" for ground truth
mode="pred"
# mode="gold"


# logging="false" for no logging, "save" for saving the status.jsonl, "true" for logging for each instance
# logging="false"
logging="save"
# logging="true"

# limit=n for limiting the number of instances to evaluate
# limit=1

python /app/src/wrapper_evaluation_${dialect}.py --jsonl_file "$jsonl_file"  --logging "$logging" --mode "$mode" \
    # --limit $limit

