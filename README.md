# MINI-SQL-ENGINE
#### Description
a mini sql engine which will run a subset of SQL Queries using ​ command line interface​ .
Type of Queries implemented here:​

    Select all records : Select * from table_name;

    Aggregate functions: Simple aggregate functions on a single column. Sum, average, max and min. They will be very trivial given that the data is only numbers: select max(col1) from table1;

    Project Columns(could be any number of columns) from one or more tables : Select col1, col2 from table_name;

    Select/project with distinct from one table : select distinct(col1), distinct(col2) from table_name;

    Select with where from one or more tables: select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20; a. In the where queries, there would be a maximum of one AND/OR operator with no NOT operators.

    Projection of one or more(including all the columns) from two tables with one join condition :
    a. select * from table1, table2 where table1.col1=table2.col2; b. select col1,col2 from table1,table2 where table1.col1=table2.col2;

``Credits``
#### [moz_sql_parser]: https://github.com/mozilla/moz-sql-parser

