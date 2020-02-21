dbname="simple_join256"
dropdb $dbname
createdb $dbname
psql $dbname -f simple_join256.sql
