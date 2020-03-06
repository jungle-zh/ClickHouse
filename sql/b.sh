#clickhouse-client --query="select name,sum(age) from user group by name "
#clickhouse-client --query="select name from user "
#clickhouse-client --query="select name from (select age,name from user)  ;"
#clickhouse-client --query="select name,sum(age) from (select age,name from user) group by name;"
#clickhouse-client --query="select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id"
#clickhouse-client --query="select user_id,name,item_id,item_price from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select item_id ,item_price from item)  t4 using item_id;"

clickhouse-client --query="select user_id,name,item_id,app_type from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select user_id ,app_type from ( select user_id,name from user ) t5 all left  join (select user_id ,app_type from user_app) t6 using  user_id )  t4 using user_id;"

#clickhouse-client --query=" select user_id, name ,app_type from (select user_id,name,count() from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select item_id ,item_price from item)  t4 using item_id group by user_id,name ) t5 all left join ( select user_id, app_type from user_app ) t6 using user_id "

#clickhouse-client --query="select  user_id ,name ,count()  from  user group by user_id,name; "

#clickhouse-client --query="select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id "
#clickhouse-client --query=" select item_id ,item_price from item "

###clickhouse-client --query="select name,count() from (select name from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select item_id ,item_price from item)  t4 using item_id ) group by name"
###clickhouse-client --query="select item_id,count() from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select item_id ,item_price from item)  t4 using item_id group by item_id"
#clickhouse-client --query="select item_id ,count() from  (select item_id  from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select item_id ,item_price from item)  t4 using item_id) group by item_id"
#clickhouse-client --query="select user_id, name, item_id, item_price from (select user_id,name,item_id from  ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) t3 all left join ( select item_id ,item_price from item)  t4 using item_id "


#clickhouse-client --query="select user_id,name,item_id  from ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id "
#clickhouse-client --query="select item_id ,count() from ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id group by item_id "
#clickhouse-client --query="select item_id ,count() from (select user_id,name,item_id  from ( select user_id,name from user) t1 all left  join (select user_id, item_id from order ) t2 using user_id ) group by item_id "
