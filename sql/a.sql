
--create table stu(name String, age UInt32 ) Engine=TinyLog()
--insert into table stu(name,age) values ('jack',20),('jack',30),('tom',10),('tom',15)
--select name,sum(age) from stu group by name ;

--create table user(user_id UInt32, sex String , name String , age UInt32 ) Engine=MergeTree() PARTITION BY sex ORDER BY user_id SETTINGS index_granularity = 5 ;
--insert into table user(user_id,sex,name,age) values (0,'m','jack0',10);
--insert into table user(user_id,sex,name,age) values (1,'m','jack1',11);
--insert into table user(user_id,sex,name,age) values (2,'m','jack2',12);
--insert into table user(user_id,sex,name,age) values (3,'m','jack3',13);
--insert into table user(user_id,sex,name,age) values (4,'m','jack4',14);
--insert into table user(user_id,sex,name,age) values (5,'m','jack5',15);
--insert into table user(user_id,sex,name,age) values (6,'m','jack6',16);
--insert into table user(user_id,sex,name,age) values (7,'f','tom0',17);
--insert into table user(user_id,sex,name,age) values (8,'f','tom1',18);
--insert into table user(user_id,sex,name,age) values (9,'f','tom2',19);
--insert into table user(user_id,sex,name,age) values (10,'f','tom3',20);
--insert into table user(user_id,sex,name,age) values (11,'f','tom4',21);
--insert into table user(user_id,sex,name,age) values (12,'f','tom5',22);
--
--create table order(id  UInt32 ,user_id UInt32, item_id UInt32,item_count UInt32 ) Engine=MergeTree()  ORDER BY id SETTINGS index_granularity = 5 ;
--insert into table order(id, user_id,item_id,item_count) values (0,0,1001,1);
--insert into table order(id, user_id,item_id,item_count) values (1,0,1003,1);
--insert into table order(id, user_id,item_id,item_count) values (2,0,1005,1);
--insert into table order(id, user_id,item_id,item_count) values (3,0,1007,1);
--insert into table order(id, user_id,item_id,item_count) values (4,1,1003,1);
--insert into table order(id, user_id,item_id,item_count) values (5,1,1004,1);
--insert into table order(id, user_id,item_id,item_count) values (6,1,1005,1);
--insert into table order(id, user_id,item_id,item_count) values (7,2,1002,1);
--insert into table order(id, user_id,item_id,item_count) values (8,2,1004,1);
--insert into table order(id, user_id,item_id,item_count) values (9,2,1006,1);
--insert into table order(id, user_id,item_id,item_count) values (10,2,1008,1);
--insert into table order(id, user_id,item_id,item_count) values (11,3,1001,1);
--insert into table order(id, user_id,item_id,item_count) values (12,3,1003,2);
--insert into table order(id, user_id,item_id,item_count) values (13,3,1006,1);
--insert into table order(id, user_id,item_id,item_count) values (14,4,1002,1);
--insert into table order(id, user_id,item_id,item_count) values (15,4,1004,1);
--insert into table order(id, user_id,item_id,item_count) values (16,4,1005,1);
--insert into table order(id, user_id,item_id,item_count) values (17,4,1006,1);


--create table item(id  UInt32 , item_id UInt32,item_price UInt32 ) Engine=MergeTree()  ORDER BY id SETTINGS index_granularity = 5 ;
--insert into table item(id, item_id,item_price) values (0,1001,100);
--insert into table item(id, item_id,item_price) values (0,1002,200);
--insert into table item(id, item_id,item_price) values (0,1003,200);
--insert into table item(id, item_id,item_price) values (0,1004,200);
--insert into table item(id, item_id,item_price) values (0,1005,500);
--insert into table item(id, item_id,item_price) values (0,1006,300);
--insert into table item(id, item_id,item_price) values (0,1007,100);
--insert into table item(id, item_id,item_price) values (0,1008,300);

create table  user_app(app_id UInt32 , user_id UInt32 , app_type UInt32) Engine=MergeTree() order by  app_id SETTINGS index_granularity = 5 ;
insert into table user_app(app_id,user_id,app_type) values (1,1,10001);
insert into table user_app(app_id,user_id,app_type) values (2,1,10002);
insert into table user_app(app_id,user_id,app_type) values (3,1,10001);
insert into table user_app(app_id,user_id,app_type) values (4,2,10002);
insert into table user_app(app_id,user_id,app_type) values (5,2,10001);
insert into table user_app(app_id,user_id,app_type) values (6,2,10002);
insert into table user_app(app_id,user_id,app_type) values (7,2,10001);
insert into table user_app(app_id,user_id,app_type) values (8,3,10002);
insert into table user_app(app_id,user_id,app_type) values (9,3,10001);
insert into table user_app(app_id,user_id,app_type) values (10,4,10002);
insert into table user_app(app_id,user_id,app_type) values (11,4,10001);
insert into table user_app(app_id,user_id,app_type) values (12,4,10002);
insert into table user_app(app_id,user_id,app_type) values (13,4,10001);
insert into table user_app(app_id,user_id,app_type) values (14,4,10002);
