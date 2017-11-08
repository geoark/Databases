drop database if exists dbdesign2017;
create database dbdesign2017;
use dbdesign2017;



create table Dat 
  (  customer_id bigint,
     birth_date date,
     afm bigint,
     contract_id varchar(15) not null,
     signature_date date,
	 limit_amount bigint,
     contract_type varchar(20) not null,
     account_id bigint,
     starting_date date,
     status varchar(13) not null,
     product_code bigint,
     collateral_id bigint,
     collateral_type varchar(1) not null,
	 collateral_amount bigint,
     collateral_end date,
     collateral_relation_type varchar(1) not null,
     real_estate_id bigint,
     appreciation_value bigint,
     appreciation_date date,
     property_type varchar(1)
     
     
  );
  
  create table Bal 
  (  customer_id bigint,
     birth_date date,
     afm bigint,
     contract_id varchar(15) not null,
     signature_date date,
	 limit_amount bigint,
     contract_type varchar(16) not null,
     account_id bigint,
     starting_date date,
     status varchar(13) not null,
     product_code bigint,
     balance_value bigint,
     balance_date date,
     balance_type varchar(8)
    
     
  );
  
  create table customers2
  (  customer_id bigint,
     birth_date date,
     afm bigint,
     primary key (customer_id)
     
  );
  
  create table contracts2 
  (  contract_id varchar(15) not null,
     signature_date date,
	 limit_amount bigint,
     contract_type varchar(20) not null,
     constraint con_ctype check ( contract_type in ( 'Omologiako','Xamilis Ekkinisis', 'Tokoxrewlytiko', 'Xreolytiko', 'Step Up')),
     primary key (contract_id)
  );
  
  create table accounts2
  (  customer_id bigint,
     contract_id varchar(15) not null,
     account_id bigint,
	 starting_date date,
     status varchar(13) not null,
     product_code bigint,
     constraint con_stat check ( status in ( 'Se Episfaleia', 'Energo', 'Diagegramenos', 'Dikastiko', 'Kleisto')),
     primary key (account_id),
     foreign key (customer_id) references customers2(customer_id)
     
  );
  
  
  
  create table collaterals 
  (  customer_id bigint,
     contract_id varchar(15) not null,
     account_id bigint,
     collateral_id bigint,
     collateral_type varchar(1) not null,
	 collateral_amount bigint,
     collateral_end date,
     collateral_relation_type varchar(1) not null,
     real_estate_id bigint,
     appreciation_value bigint,
     appreciation_date date,
     property_type varchar(1),
     constraint con_protype check ( property_type in ( '1', '2', '3', '4' )),
     constraint con_reltype check ( collateral_relation_type in ( '1', '2', '3' )),
     primary key (collateral_id),
     foreign key (account_id) references accounts2(account_id),
     foreign key (customer_id) references customers2(customer_id),
     foreign key (contract_id) references contracts2(contract_id)
  );
     
      
	
  
  create table customers1
  (   customer_id bigint,
      birth_date date,
      afm bigint,
	  primary key (customer_id)
      
  );
  
    create table accounts1
  (  customer_id bigint,
     contract_id varchar(15) not null,
     account_id bigint,
     starting_date date,
     status varchar(13) not null,
     product_code bigint,
     constraint con_stat check ( status in ( 'Se Episfaleia', 'Energo', 'Diagegramenos', 'Dikastiko', 'Kleisto')),
     primary key (account_id),
	 foreign key (customer_id) references customers1(customer_id)
     
     
  );
  
  create table contracts1
  (  contract_id varchar(15) not null,
     signature_date date,
	 limit_amount bigint, 
     contract_type varchar(20) not null,
	 constraint con_ctype check ( contract_type in ( 'Omologiako','Xamilis Ekkinisis', 'Tokoxrewlytiko', 'Xreolytiko', 'Step Up')),
	 primary key (contract_id)
      
  );
  
  create table balances
  (  balance_value bigint,
     balance_date date,
     balance_type varchar(8),
     constraint con_btype check ( balance_type in ( 'Capital', 'Interest', 'Expenses')),
     primary key (balance_value)
     
  );
  
  create table t1
  (  account_id bigint,
	 balance_value bigint,
	 primary key (account_id,balance_value)
     
  );
  
  load data local infile 'C:/Data/bal.txt'
  into table Bal
  fields terminated by '@'
  lines terminated by '\r\n'
  ignore 1 rows
  (customer_id,@date1,afm,contract_id,@date2,limit_amount,contract_type,account_id,@date3,status,product_code,balance_value,@date4,balance_type)
  set
  birth_date = str_to_date(@date1, '%d/%m/%Y'),
  signature_date = str_to_date(@date2, '%d/%m/%Y'),
  starting_date = str_to_date(@date3, '%d/%m/%Y'),
  balance_date = str_to_date(@date4, '%d/%m/%Y');
  
  load data local infile 'C:/Data/dat.txt'
  into table Dat
  fields terminated by '@'
  lines terminated by '\r\n'
  ignore 1 rows
  (customer_id,@date1,afm,contract_id,@date2,limit_amount,contract_type,account_id,@date3,status,product_code,collateral_id,collateral_type,collateral_amount,@date4,collateral_relation_type,real_estate_id,appreciation_value,@date5,property_type)
  set
  birth_date = str_to_date(@date1, '%d/%m/%Y'),
  signature_date = str_to_date(@date2, '%d/%m/%Y'),
  starting_date = str_to_date(@date3, '%d/%m/%Y'),
  collateral_end = str_to_date(@date4, '%d/%m/%Y'),
  appreciation_date = str_to_date(@date5, '%d/%m/%Y');
  
  insert into customers1 
  select distinct customer_id,birth_date,afm
  from Bal;
  
  insert into accounts1 
  select distinct customer_id,contract_id,account_id,starting_date,status,product_code
  from Bal;
  
  insert into contracts1 
  select distinct contract_id,signature_date,limit_amount,contract_type
  from Bal;
  
  insert into balances 
  select distinct balance_value,balance_date,balance_type
  from Bal;
  
  insert into t1 
  select distinct account_id,balance_value
  from Bal;
  
  insert into customers2
  select distinct customer_id,birth_date,afm
  from Dat;
  
  insert into accounts2
  select distinct customer_id,contract_id,account_id,starting_date,status,product_code
  from Dat;
  
  insert into contracts2
  select distinct contract_id,signature_date,limit_amount,contract_type
  from Dat;
  
  insert into collaterals
  select distinct customer_id,contract_id,account_id,collateral_id,collateral_type,collateral_amount,collateral_end,collateral_relation_type,real_estate_id,appreciation_value,appreciation_date,property_type
  from Dat;
 
  

select count(*) as totalcust1, contracts1.contract_id,signature_date,birth_date,customers1.customer_id
from customers1,contracts1,accounts1
where customers1.customer_id=accounts1.customer_id and contracts1.contract_id=accounts1.contract_id and
extract(year from signature_date) -extract(year from birth_date) < 19;

select count(*) as totalcust2,contracts2.contract_id,signature_date,birth_date,customers2.customer_id
from customers2,contracts2,accounts2
where customers2.customer_id=accounts2.customer_id and contracts2.contract_id=accounts2.contract_id and
extract(year from signature_date) -extract(year from birth_date) < 19;

select count(*) as totalcust3,customers2.customer_id,birth_date,property_type
from customers2,collaterals
where customers2.customer_id=collaterals.customer_id and property_type='1' 
and 2017- extract(year from birth_date) >60;

select curdate() currentdate,customers1.customer_id,accounts1.account_id,balance_type,count(*) as totalcust4,balance_value
from customers1,accounts1,balances
where customers1.customer_id=accounts1.customer_id and  balance_type='Capital' and balance_value > 100000;

select count(*) as totalamountcolid, sum(collateral_amount), accounts2.account_id, collaterals.collateral_id
from collaterals,accounts2
where accounts2.account_id=collaterals.account_id;


  
  
  
  
     
      
      
     
     
     

