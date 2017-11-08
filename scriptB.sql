drop database if exists projectB;
create database projectB;
use projectB;

-- CREATE SCHEMA TABLES

create table customer (
	customer_id varchar(9),
	birth_date date,
	afm varchar(9),
	PRIMARY KEY (customer_id)
);

create table contract (
	contract_id varchar(14),
	signature_date date,
	limit_amount decimal(10,2),
	contract_type varchar(17),
	customer_id varchar(9),
	PRIMARY KEY (contract_id),
	FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
);

create table account (
	account_id varchar(15),
	starting_date date,
	status varchar(13),
	product_code varchar(4),
	contract_id varchar(14),
	PRIMARY KEY (account_id),
	FOREIGN KEY (contract_id) REFERENCES contract (contract_id)
);

create table collateral (
	collateral_id varchar(10),
	collateral_type int,
	collateral_amount decimal(10,2),
	collateral_end date,
	collateral_relation_type int,
	customer_id varchar(9),
	contract_id varchar(14),
	account_id varchar(15),
	PRIMARY KEY (collateral_id),
	FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
	FOREIGN KEY (contract_id) REFERENCES contract (contract_id),
	FOREIGN KEY (account_id) REFERENCES account (account_id)
);

create table real_estate (
	real_estate_id varchar(10),
	appreciation_value decimal(10,2),
	appreciation_date date,
	property_type int,
	collateral_id varchar(10),
	PRIMARY KEY (real_estate_id),
	FOREIGN KEY (collateral_id) REFERENCES collateral (collateral_id)
);

create table balance (
	account_id varchar(15),
	balance_value decimal(10,2),
	balance_date date,
	balance_type varchar(8),
    FOREIGN KEY (account_id) REFERENCES account (account_id)
    
    
);

-- CREATE TEMPORARY DATA LOAD TABLES

create table dat_table (
	customer_id varchar(9),
	birth_date date,
	afm varchar(9),
	contract_id varchar(14),
	signature_date date,
	limit_amount decimal(10,2),
	contract_type varchar(17),
	account_id varchar(15),
	starting_date date,
	status varchar(13),
	product_code varchar(4),
	collateral_id varchar(10),
	collateral_type int,
	collateral_amount decimal(10,2),
	collateral_end date,
	collateral_relation_type int,
	real_estate_id varchar(10),
	appreciation_value decimal(10,2),
	appreciation_date date,
	property_type int
);

create table bal_table (
	customer_id varchar(9),
	birth_date date,
	afm varchar(9),
	contract_id varchar(14),
	signature_date date,
	limit_amount decimal(10,2),
	contract_type varchar(17),
	account_id varchar(15),
	starting_date date,
	status varchar(13),
	product_code varchar(4),
	balance_value decimal(10,2),
	balance_date date,
	balance_type varchar(8)
);

load data local infile 'C:/Data/dat.txt'
into table dat_table 
fields terminated by '@' 
lines terminated by '\r\n'
ignore 1 rows
(customer_id, @var1, afm, contract_id, @var2, limit_amount, contract_type, account_id, @var3, status, product_code, collateral_id, collateral_type, collateral_amount, @var4, collateral_relation_type, real_estate_id, appreciation_value, @var5, property_type)
set
birth_date = STR_TO_DATE(@var1, '%d/%m/%Y'),
signature_date = STR_TO_DATE(@var2, '%d/%m/%Y'),
starting_date = STR_TO_DATE(@var3, '%d/%m/%Y'),
collateral_end = STR_TO_DATE(@var4, '%d/%m/%Y'),
appreciation_date = STR_TO_DATE(@var5, '%d/%m/%Y');

load data local infile 'C:/Data/bal.txt'
into table bal_table 
fields terminated by '@' 
lines terminated by '\r\n'
ignore 1 rows
(customer_id, @var1, afm, contract_id, @var2, limit_amount, contract_type, account_id, @var3, status, product_code, balance_value, @var4, balance_type)
set
birth_date = STR_TO_DATE(@var1, '%d/%m/%Y'),
signature_date = STR_TO_DATE(@var2, '%d/%m/%Y'),
starting_date = STR_TO_DATE(@var3, '%d/%m/%Y'),
balance_date = STR_TO_DATE(@var4, '%d/%m/%Y');


-- LOAD DATA FROM IMPORT TABLE TO SCHEMA TABLES

insert into customer
select distinct customer_id, birth_date, AFM
from dat_table;

insert into contract
select distinct contract_id, signature_date, limit_amount, contract_type, customer_id
from dat_table;

insert into account
select distinct account_id, starting_date, status, product_code,  contract_id
from dat_table;

insert into collateral
select distinct collateral_id, collateral_type, collateral_amount, 
collateral_end, collateral_relation_type, 
case when collateral_relation_type=1 then
customer_id end, 
case when collateral_relation_type=2 then
contract_id end,
case when collateral_relation_type=3 then
account_id end 
from dat_table;

insert into real_estate
select distinct real_estate_id, appreciation_value, appreciation_date, property_type, collateral_id
from dat_table
where real_estate_id is not null;

insert into balance
select distinct account_id, balance_value, balance_date, balance_type
from bal_table;

-- CREATE TEMPORARY DATA LOAD TABLES

create table dat2_table (
	customer_id varchar(9),
	birth_date date,
	afm varchar(9),
	contract_id varchar(14),
	signature_date date,
	limit_amount decimal(10,2),
	contract_type varchar(17),
	account_id varchar(15),
	starting_date date,
	status varchar(13),
	product_code varchar(4),
	collateral_id varchar(10),
	collateral_type int,
	collateral_amount decimal(10,2),
	collateral_end date,
	collateral_relation_type int,
	real_estate_id varchar(10),
	appreciation_value decimal(10,2),
	appreciation_date date,
	property_type int
);

create table bal2_table (
	customer_id varchar(9),
	birth_date date,
	afm varchar(9),
	contract_id varchar(14),
	signature_date date,
	limit_amount decimal(10,2),
	contract_type varchar(17),
	account_id varchar(15),
	starting_date date,
	status varchar(13),
	product_code varchar(4),
	balance_value decimal(10,2),
	balance_date date,
	balance_type varchar(8)
);

-- INDEXES
create unique index bal_index using btree on balance(account_id,balance_value,balance_date,balance_type);
create unique index col_index using btree on collateral(account_id,customer_id,contract_id,collateral_id,collateral_type,collateral_amount,collateral_end,collateral_relation_type);


load data local infile 'C:/Data/dat2.txt'
into table dat2_table 
fields terminated by '@' 
lines terminated by '\r\n'
ignore 1 rows
(customer_id, @var1, afm, contract_id, @var2, limit_amount, contract_type, account_id, @var3, status, product_code, collateral_id, collateral_type, collateral_amount, @var4, collateral_relation_type, real_estate_id, appreciation_value, @var5, property_type)
set
birth_date = STR_TO_DATE(@var1, '%d/%m/%Y'),
signature_date = STR_TO_DATE(@var2, '%d/%m/%Y'),
starting_date = STR_TO_DATE(@var3, '%d/%m/%Y'),
collateral_end = STR_TO_DATE(@var4, '%d/%m/%Y'),
appreciation_date = STR_TO_DATE(@var5, '%d/%m/%Y');

load data local infile 'C:/Data/bal2.txt'
into table bal2_table 
fields terminated by '@' 
lines terminated by '\r\n'
ignore 1 rows
(customer_id, @var1, afm, contract_id, @var2, limit_amount, contract_type, account_id, @var3, status, product_code, balance_value, @var4, balance_type)
set
birth_date = STR_TO_DATE(@var1, '%d/%m/%Y'),
signature_date = STR_TO_DATE(@var2, '%d/%m/%Y'),
starting_date = STR_TO_DATE(@var3, '%d/%m/%Y'),
balance_date = STR_TO_DATE(@var4, '%d/%m/%Y');

-- LOAD DATA FROM IMPORT TABLE TO SCHEMA TABLES

insert into customer
select distinct customer_id, birth_date, AFM
from dat2_table;

insert into contract
select distinct contract_id, signature_date, limit_amount, contract_type, customer_id
from dat2_table;

insert into account
select distinct account_id, starting_date, status, product_code,  contract_id
from dat2_table;

insert into collateral
select distinct collateral_id, collateral_type, collateral_amount, 
collateral_end, collateral_relation_type, 
case when collateral_relation_type=1 then
customer_id end, 
case when collateral_relation_type=2 then
contract_id end,
case when collateral_relation_type=3 then
account_id end 
from dat2_table;

insert into real_estate
select distinct real_estate_id, appreciation_value, appreciation_date, property_type, collateral_id
from dat2_table
where real_estate_id is not null;

insert into balance
select distinct account_id, balance_value, balance_date, balance_type
from bal2_table;


-- QUERY 1
select count(distinct account.account_id) as total1 
from balance,collateral,account
where account.account_id=balance.account_id and account.account_id=collateral.account_id and 
balance.balance_type= 'Capital' and collateral.collateral_relation_type=3 and 
balance.balance_value > collateral.collateral_amount;

-- QUERY 2
select (sum(balance.balance_value)/count(balance.account_id)) as total2 
from balance,account,
  (select max(balance.balance_date) as max1,balance.account_id
	from balance
    where EXTRACT(YEAR FROM balance_date) <= '2006'
    group by balance.account_id) as Max
where balance.account_id=Max.account_id and balance.balance_date=Max.max1 
and balance.account_id=account.account_id and balance.balance_type = 'Capital';             

-- QUERY 3 
select count(collateral.collateral_id) as total3
from collateral left join account on account.account_id=collateral.account_id
                 left join customer on customer.customer_id=collateral.customer_id and
extract(year from curdate())- extract(year from birth_date) > 60
                 left join balance on account.account_id=balance.account_id and 
balance.balance_value <= 500000 and balance.balance_type='Capital'
where collateral.collateral_amount >= 1000000 and collateral.collateral_relation_type=2
order by collateral.collateral_id;		
 
-- QUERY 4 
select count(account.account_id) as total4
from account left join collateral on account.account_id=collateral.account_id and
collateral.collateral_type=1 
              left join  balance on account.account_id=balance.account_id and
balance.balance_value <= 10000 and balance.balance_type='Interest'
              left join real_estate on collateral.collateral_id=real_estate.collateral_id and
real_estate.appreciation_value >= 100000
order by account.account_id ; 

-- DROP IMPORT TABLES
drop table dat_table;
drop table bal_table;
drop table dat2_table;
drop table bal2_table;

-- END OF CODE  
 


  