  -- A look at the datasets 
  select * from sales;
  select * from sales2; 
  
  -- Data Cleaning
  -- 1. Standarized Column data type
  -- 2. Extract and seperate mixed data
  -- 3. Remove Dupicates
  
  Create table Sales(ID Int Null, Date varchar(50) null,Month Varchar(50),
Customer Varchar(50), Style Varchar(50), Sku Varchar(50), Size Varchar(50), 
PCS Varchar(50), Rate Varchar(50), Gross_Amt Varchar(50));

-- To import Sales Data from csv into the Sales table
set global local_infile =1;
Load data infile'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PReport.csv'
into table sales
fields terminated by','
optionally enclosed by'"'
lines terminated by'\n'
ignore 1 rows;   -- To ignore the header in the Sales data

-- To split second dataset into a new table
Create table Sales2 as select * from Sales where id>=19675;
Create table Sales2_backup as select * from Sales2;

-- Delete second dataset from Sales table
Delete from sales where id>=19675;

-- Recreate Id Column to start from 1
Alter table sales drop column Id;
Alter table sales add column ID Int NOT NULl Auto_Increment Primary Key First;

-- 
Select * from sales where str_to_date(date,'%m-%d-%Y') is  null and date <> ''; -- 1039 rows containing text found
Delete from sales  where  id between 18371 and 19675;

-- Standarize the Date column to a valid date data type
Update sales set Date = str_to_date(Date,'%m-%d-%Y');
Alter table sales modify column Date date ;
Select distinct Date from sales;

-- Update month column
Update Sales set Month = date_format(date,'%M-%Y');
Select distinct month from sales; -- No outliers found

-- Standarlized Customers name by removing spaces 
Select distinct customer from Sales; -- 148 was returned, 145 rows was returned after trimming
Select distinct customer from sales where customer <> trim(customer); -- 11 rows where found
Update sales set customer=trim(customer) where customer<>trim(customer);
Update sales set customer=trim(trailing '.' from customer);

-- Standarlized customer name by correcting spelling inconsistencies
Select distinct customer, count(*) from Sales group by customer order by 1; -- 6 outliners was found
Update sales set Customer = case 
when Customer ='ARKH FASHION' then 'ARKH FASHION BOUTIQUE PTY LTD'
when Customer ='BISSY SKARIA' then 'BINCY SKARIA'
when Customer = 'Fusion Fashions Corp. (Gopikas)' then 'FUSION FASHIONS CORP.'
when Customer ='Harsiniy Kumar' then 'HARSINIY KUMARESON'
when Customer ='MONIYSHAA' then 'MONISYAA'
when Customer ='Vinodha' then 'VINODHA PUSPANATHAN'
else customer
End;
Update sales set customer = upper(customer); -- 139 were affected

-- Finding Style Outliners
Select distinct style, count(*) from Sales group by Style order by Style; -- No outliners found

-- Finding Sku Outliners
Select distinct sku, count(*) from Sales group by 1 order by 1; -- 1172 blanks found
Update sales set Sku = null where sku = '';

-- Finding Size Outliners
Select distinct size, count(*) from Sales group by 1 order by 1; -- No outliners 

-- Finding Pcs Outliners
Select distinct pcs, count(*) from Sales group by 1 order by 1; -- No outliners found

-- Update pcs column to a valid data type
Alter table sales modify Pcs int;

-- Finding Rate Outliners
Select distinct Rate, count(*) from Sales group by 1 order by 1; -- No outliners found

-- Update Rate column to a valid data type
Alter table Sales modify Rate decimal(10,2);

-- Finding Gross_amt Outliners
Select distinct Grosss_Amt, count(*) from Sales group by 1 order by 1;

-- Update Gross_amt column to a valid data type
Alter table sales modify Grosss_Amt int;

-- Finding duplicate
Select * from ( select *, row_number()  over(partition by
customer,date,month,style,Sku,size,pcs,rate,Grosss_Amt order by id) Duplicate 
from sales) S  where Duplicate >1;                          -- 6290 Duplicate found

Delete from sales where id In (select id from (select id, row_number() 
over (partition by date,month,customer,style,Sku,size,pcs,rate,Grosss_Amt order by id) as Duplicate
from sales) as t where Duplicate > 1);

-- Second Table Cleaning

-- To Rename the Column Header
Alter table sales2 rename column Customer to Months;
Alter table sales2 rename column Date to Customer;
Alter table sales2 rename column Month to Date;
Alter table sales2 rename column Style to Style; 
Alter table sales2 rename column Sku to Sku;
Alter table sales2 rename column Grosss_Amt to Stock;
Alter table sales2 rename column Rate to Gross_Amt;
Alter table sales2 rename column Pcs to Rate;
Alter table sales2 rename column Size to Pcs;

-- To Remove Second Header
Delete from sales2 where id=19675;

-- Recreate Id Column to start from 1
Alter table sales2 drop column Id;
Alter table sales2 add column ID Int NOT NULl Auto_Increment Primary Key First;

-- To spot inconsistencies in the Customer Column
Select distinct customer,count(*) from sales2 order by customer;
Select distinct customer from sales2 where customer <> trim(customer); -- 10 rows where found
Update sales2 set customer=trim(customer) where customer<>trim(customer);
Select customer, count(*) as new from sales2 group by customer order by 1;
Update sales2 set Customer = case 
when Customer ='ARKH FASHION' then 'ARKH FASHION BOUTIQUE PTY LTD'
when Customer ='MONIYSHAA' then 'MONISYAA'
else customer
End;
Update sales2 set customer=trim(trailing '.' from customer);

-- Standarize the Date column to a valid date data type
Update sales2 set Date=str_to_date(Date,'%m-%d-%Y');
Alter table sales2 modify column Date date;
select distinct Date from Sales2;

-- Update month column
Update Sales2 set Months = date_format(date,'%M-%Y');

-- Finding Style Outliers
Select distinct Style, count(*) from sales2 group by 1 order by style;   -- 3 outliners found
Update sales2 set Style = case 
when Style = 'SHIPPING CHARGES'  then 'SHIPPING'
when Style = 'TAG PRINTING' then 'TAGS'
when Style = 'TAGS(LABOUR)' then 'TAGS'
else Style
End;

-- Finding Sku Outliers
Select distinct Sku, count(*) from sales2 group by 1 order by 1; -- 4 outliners found
Update sales2 set Sku = case 
when Sku = 'SHIPPING CHARGES'  then 'SHIPPING'
when Sku = 'TAG PRINTING' then 'TAGS'
when Sku = 'TAGS(LABOUR)' then 'TAGS'
when Sku = '' then null
else Sku
End;

-- Finding Pcs Outliers
Select distinct Pcs, count(*) from sales2 group by 1 order by 1; -- No outliners found

-- Update Pcs column to a valid data type
Alter table sales2 modify Pcs int;

-- Finding Rate Outliners
Select distinct Rate, count(*) from sales2 group by 1 order by 1; -- No outliners found

-- Update Rate column to a valid data type
Alter table Sales2 modify Rate decimal(10,2);

-- Finding Gross_Amt Outliners
Select distinct Gross_Amt, count(*) from sales2 group by 1 order by 1; -- No outliners found

-- Update Gross_Amt column to a valid data type
Alter table sales2 modify Gross_Amt int;

-- Finding Stock Outliners
Select distinct Stock, count(*) from sales2 group by 1 order by 1; -- No outliners found

-- Update Stock column to a valid data type
Alter table sales2 modify Stock int;

-- Finding duplicate
Select * from ( select *, row_number()  over(partition by
customer,date,months,style,Sku,pcs,Gross_Amt,stock order by id) Duplicate 
from sales2) S  where Duplicate >1;                          -- 6579 Duplicate found

Delete from sales2 where id In (select id from (select id, row_number() 
over (partition by customer,date,months,style,Sku,pcs,Gross_Amt,stock order by id) as Duplicate
from sales2) as t where Duplicate > 1);





