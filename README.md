# SNPIAO
Project - Generating ETL code

Generate SQL code for ETL processes for SCD1 and SCD2.

The problem at hand is to generate SQL commands for ETL process for a given operational database. Because of the high variability of the structure of operational databases, the python scripts are used to create a more general data warehouse structure, which can be modified later for a better representation of the solution for the given task. The user also must be aware of the stucture of the ODB such that all of the columns that are used for input should be from tables that are in a direct relationship.
Adventure Works 2016 is the database used for testing the script, the following columns were used as an input for columns of interest: Employee.VacationHours, Employee.SickLeaveHours, EmployeePayHistory.PayFrequency, EmployeePayHistory.Rate, SalesPerson.SalesLastYear (data from these columns is used to interpret the solution for the given problem). 
As mentioned above all these columns are from tables that are in a direct relationship (in the example input, all of the tables are linked with the column - BusinessEntityID).
The fact table is made out of the columns that are used as an input and the key columns that are linking the dimension tables.

The steps used for creating the data warehouse were:

- create dimension tables consisted of columns from the tables of interest (excluding the columns that are going to be in the fact table)
- create the fact table
- create a temporary extended fact table used for the initial insertion of data and Key alignment 
- insertion of data in the temporary fact table
- insertion of data in the dimension tables from the temporary fact table
- update the dimension keys in the temporary fact table
- insertion of data in the fact table from the temporary table
