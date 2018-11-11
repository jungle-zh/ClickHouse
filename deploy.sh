
cd /Users/admin/work/workspace/Clickhouse/dbms
rm -rf src.tar 
tar czvf src.tar src 
scp -i ~/id_clickhouse src.tar  ubuntu@52.83.200.179:
