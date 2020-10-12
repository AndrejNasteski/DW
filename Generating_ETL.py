import csv


def scd1_create_table(columns, fact_columns):
    """
    :param fact_columns:
    columns from this table that are used in the fact table
    :param columns:
     [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:

    CREATE TABLE dim.[table]_SCD1
    (SKEY BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    [table attributes]
    FirstLoadDate DATETIME NOT NULL,
    LastChangeDate DATETIME NOT NULL
    )
    """

    command = "CREATE TABLE dim." + columns[1][1] + "_SCD1\n" + \
              "(SKey BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL,\n"
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = fact_columns  # filter column names that should not be in the dimension table
    column_info = []  # extended list also containing the type of data

    for column in columns:
        if column[2] in column_names and column[5] == "False":
            continue
        if column[3] in data_types and column[5] == "False":
            continue
        column_info.append([column[2], column[3], column[1]])  # column name, type, table name
        column_command = "" \
                         + str(column[2]) + " " \
                         + str(column[3]).upper()
        if column[4] == "True":
            column_command += " NOT NULL"
        else:
            column_command += " NULL"
        if column[6] == "True":
            column_command += " UNIQUE"
        column_command += ',\n'
        command += column_command

    command += "FirstLoadDate DATETIME NOT NULL,\n" \
               + "LastChangeDate DATETIME NOT NULL\n)"
    column_info.append(["FirstLoadDate", "DATETIME", columns[0][1]])
    column_info.append(["LastLoadDate", "DATETIME", columns[0][1]])

    return [command, column_info]


def scd2_create_table(columns, fact_columns):
    """
    :param fact_columns:
        columns from this table that are used in the fact table
    :param columns:
     [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:

    CREATE TABLE dim.[table]_SCD2
    (SKEY BIGINT IDENTITY(1,1) NOT NULL,
    [table attributes]
    IsCurrent BIT NOT NULL,
    IsDeleted BIT NULL,
    RowVersion INT NOT NULL,
    ValidFrom DATETIME NOT NULL,
    ValidTo DATETIME NULL
    )
    """

    command = "CREATE TABLE dim." + columns[1][1] + "_SCD2\n" + \
              "(SKey BIGINT IDENTITY(1,1) NOT NULL,\n"
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = fact_columns  # filter column names that should not be in the dimension table
    column_info = []

    for column in columns:
        if column[2] in column_names and column[5] == "False":
            continue
        if column[3] in data_types and column[5] == "False":
            continue
        column_info.append([column[2], column[3], column[1]])  # column name, type, table name
        column_command = "" \
                         + str(column[2]) + " " \
                         + str(column[3]).upper()
        if column[4] == "True":
            column_command += " NOT NULL"
        else:
            column_command += " NULL"
        if column[6] == "True":
            column_command += " UNIQUE"
        column_command += ',\n'
        command += column_command

    command += "IsCurrent BIT NOT NULL,\n" \
               + "IsDeleted BIT NULL,\n" \
               + "RowVersion INT NOT NULL,\n" \
               + "ValidFrom DATETIME NOT NULL,\n" \
               + "ValidTo DATETIME NULL\n)"

    column_info.append(["IsCurrent", "BIT", columns[0][1]])
    column_info.append(["IsDeleted", "BIT", columns[0][1]])
    column_info.append(["RowVersion", "INT", columns[0][1]])
    column_info.append(["ValidFrom", "DATETIME", columns[0][1]])
    column_info.append(["ValidTo", "DATETIME", columns[0][1]])

    return [command, column_info]


def scd1_insert_row_temp(columns, fact_columns):
    """

    :param columns:
         [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :param fact_columns:
            columns from this table that are used in the fact table
    :return:
    """
    fact = [i[0] for i in fact_columns]
    command = "INSERT INTO dim." + columns[0][1] + " (\n"
    for item in columns:
        if item[2] not in fact:
            command += "\t" + item[2] + ",\n"

    command += "\tFirstLoadDate,\n\tLastLoadDate\n)\nSELECT DISTINCT\n"

    for item in columns:
        if item[2] not in fact:
            command += "\tTEMP." + item[2] + ",\n"

    command += "\tGETDATE(),\n\tGETDATE()\nFROM TEMP\nORDER BY TEMP." + columns[0][2]

    return command


def scd2_insert_row_temp(columns, fact_columns):
    """

    :param columns:
         [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :param fact_columns:
            columns from this table that are used in the fact table
    :return:
    """
    fact = [i[0] for i in fact_columns]
    command = "INSERT INTO dim." + columns[0][1] + " (\n"
    for item in columns:
        if item[2] not in fact:
            command += "\t" + item[2] + ",\n"
    command += "\tIsCurrent,\n\tIsDeleted,\n\tRowVersion,\n\tValidFrom,\n\tValidTo\n)\nSELECT DISTINCT\n"

    for item in columns:
        if item[2] not in fact:
            command += "\tTEMP." + item[2] + ",\n"

    command += "\t1, 0, 1, GETDATE(), NULL\nFROM TEMP\nORDER BY TEMP." + columns[0][2]

    return command


def scd1_insert_row(columns, database_name):
    """

    :param database_name: Name of your operational database
    :param columns: list of all rows with the same table name
    [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:
        INSERT INTO dim.[table]_SCD1
    SELECT odb.[attributes], GETDATE(), GETDATE()
    FROM [database_name].[schema].[table] AS odb
    LEFT JOIN dim.[table_SCD1] AS dwd ON odb.[table]ID = dwd.[table]ID
    WHERE dwd.SKey IS NULL

    """

    table_name = columns[1][1]
    command = "INSERT INTO dim." + table_name + "_SCD1\nSELECT"
    table_id_column = columns[1][2] + "ID"  # the first column is the default value for table primary key
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = ["ModifiedDate"]  # filter column names that should not be in the dimension table

    for item in columns:
        if item[2] in column_names and item[5] == "False":
            continue
        if item[3] in data_types and item[5] == "False":
            continue
        if item[5] == "True":
            table_id_column = item[2]

        command += " odb." + item[2] + ","

    command += " GETDATE(), GETDATE()\n" \
               + "FROM " + database_name + "." + table_name + " AS odb\n" \
               + "LEFT JOIN dim." + columns[1][1] \
               + "_SCD1 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column + "\n" \
               + "WHERE dwd.SKey IS NULL\n"

    return command


def scd2_insert_row(columns, database_name):
    """

    :param database_name: Name of your operational database
    :param columns: list of all rows with the same table name
    [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:
    INSERT INTO dim.[table]_SCD2
    SELECT odb.[attributes], 1, 0, 1, GETDATE(), NULL
    FROM [database_name].[schema].[table] AS odb
    LEFT JOIN dim.[table]_SCD2 AS dwd ON d.[table]ID = dwd.[table]ID
    WHERE dwd.SKey IS NULL

    """

    table_name = columns[1][1]
    command = "INSERT INTO dim." + table_name + "_SCD2\nSELECT"
    table_id_column = columns[1][2] + "ID"  # the first column is the default value for table primary key
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = ["ModifiedDate"]  # filter column names that should not be in the dimension table

    for item in columns:
        if item[2] in column_names and item[5] == "False":
            continue
        if item[3] in data_types and item[5] == "False":
            continue
        if item[5] == "True":
            table_id_column = item[2]

        command += " odb." + item[2] + ","

    command += "1, 0, 1, GETDATE(), NULL\n" \
               + "FROM " + database_name + "." + table_name + " AS odb\n" \
               + "LEFT JOIN dim." + columns[1][1] \
               + "_SCD1 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column + "\n" \
               + "WHERE dwd.SKey IS NULL\n"

    return command


def scd1_update_row(columns):
    """

    :param columns: list of all rows with the same table name
    [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:

    UPDATE dim.Department_SCD1
    SET Name=odb.Name, GroupName=odb.GroupName, DepartmentId=odb.DepartmentID
    FROM odb.HumanResources.Department AS odb
    WHERE dim.Department_SCD1.DepartmentID=odb.DepartmentID
    AND HASH(old_fields) != HASH(new_fields)
    """

    table_name = columns[1][1]
    command = "UPDATE dim." + table_name + "_SCD1\nSET "

    table_id_column = columns[1][2] + "ID"  # the first column is the default value for table primary key
    old_fields = ""
    new_fields = ""
    for item in columns:
        if item[5] == "True":
            table_id_column = item[2]
        old_fields += "dim." + table_name + "_SCD1." + item[2] + ", "
        new_fields += "odb." + item[2] + ", "
        command += item[2] + " = odb." + item[2] + ", "

    old_fields = old_fields[:-2]  # remove excess characters
    new_fields = new_fields[:-2]  # remove excess characters
    command = command[:-2]  # remove excess characters

    command += "\nFROM odb." + columns[0][0] + "." + table_name + " AS odb\n" \
               + "WHERE dim." + table_name + "_SCD1." + table_id_column + " = odb." + table_id_column \
               + "\nAND HASH(" + old_fields + ") != \n HASH(" + new_fields + ")"

    return command


def scd2_update_row(columns):
    """

        :param columns: list of all rows with the same table name
        [[schema, table, column1, type, not_null, primary_key, unique],
          [schema, table, column2, type, not_null, primary_key, unique], ...]
        :return:

        UPDATE dim.Department_SCD1
        SET Name=odb.Name, GroupName=odb.GroupName, DepartmentId=odb.DepartmentID
        FROM odb.HumanResources.Department AS odb
        WHERE dim.Department_SCD1.DepartmentID=odb.DepartmentID
        AND HASH(old_fields) != HASH(new_fields)
        """

    table_name = columns[1][1]
    table_id_column = ""
    old_fields = ""
    new_fields = ""
    command = "IF object_id('#SCD2') IS NOT NULL\nDROP TABLE #SCD2\n\n"
    command += "SELECT dwd.SKey, dwd.RowVersion + 1 as NewVersion,\n" \
               "\tCAST(1 AS BIT) AS NewIsCurrent,\n" \
               "\tGETDATE() AS NewValidFrom,\n" \
               "\tCAST(NULL AS DATETIME) AS NewValidTo,\n"

    for item in columns:
        if item[5] == "True":
            table_id_column = item[2]

        if item[3] == "datetime":
            continue

        command += "\todb." + item[2] + " AS New" + item[2] + ", \n"
        old_fields += "dim." + table_name + "_SCD2." + item[2] + ", "
        new_fields += "odb." + item[2] + ", "

    old_fields = old_fields[:-2]  # remove excess characters
    new_fields = new_fields[:-2]  # remove excess characters
    command = command[:-3]  # remove excess characters

    command += "\nINTO #SCD2 AS s\n" \
               "FROM odb." + columns[0][0] + '.' + table_name + " AS odb\n" \
               + "JOIN dim." + table_name + "SCD_2 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column \
               + "\nAND HASH(" + old_fields + ") != HASH(" + new_fields + ")\n" \
               + "WHERE dwd.IsCurrent = 1\n\n"

    command += "UPDATE dim." + table_name + "_SCD2\n" \
               + "SET IsCurrent = 0, ValidTo = s.NewValidFrom\n" \
               + "FROM #SCD2 AS s\n" \
               + "WHERE dim." + table_name + "_SCD2.SKey = s.SKey\n" \
               + "AND dim." + table_name + "_SCD2.IsCurrent =1\n\n"

    command += "SET IDENTITY_INSERT dim." + table_name + "_SCD2 ON\n\n" \
               + "INSERT INTO dim." + table_name + "_SCD2\n" \
               + "SELECT s.*\n FROM #SCD2 AS s\n\n" \
               + "SET IDENTITY_INSERT dim." + table_name + "_SCD2 OFF\n"

    return command


def create_fact_table(columns, fact_columns, scd):
    """

    :param columns: list of all rows with the same table name
        [[schema, table, column1, type, not_null, primary_key, unique],
          [schema, table, column2, type, not_null, primary_key, unique], ...]
    :param fact_columns: list of columns that the fact table consists of
        [column1, column2, ...]
    :param scd: SCD1 or SCD2 - must be written as a string
    :return: a list containing the sql command and the dimension tables
    """
    columns_temp = [i[0] for i in fact_columns]
    column_info = []
    tables = []
    for item in columns:
        if item[2] in columns_temp:
            tables.append(item[1])
            column_info.append([item[2], item[3]])

    tables = list(set(tables))
    command = "CREATE TABLE FACT_TABLE (\n"

    for table in tables:
        command += "\t" + table + "_ID INT NOT NULL REFERENCES dim." + table + "_" + scd + " ON DELETE CASCADE,\n"

    for column in column_info:
        command += "\t" + column[0] + " " + column[1].upper() + " NOT NULL DEFAULT 0,\n"

    command = command[:-2]
    command += "\n\tPRIMARY KEY("
    tables = [x + "_ID" for x in tables]

    for table in tables:
        command += table + " ASC, "

    command = command[:-2]
    command += ")\n);"

    return [command, tables]


def create_temp_fact(att, fact_columns, dim_columns):
    fact_columns_temp = [i[0] for i in fact_columns]
    fact_column_info = []
    tables = []
    for item in att:
        if item[2] in fact_columns_temp:
            tables.append(item[1])
            fact_column_info.append([item[2], item[3]])
    tables = list(set(tables))

    dim_column_names = []
    dim_columns_unique = []
    for item in dim_columns:
        if item[0] not in dim_column_names:
            dim_column_names.append(item[0])
            dim_columns_unique.append(item)

    command = "CREATE TABLE TEMP(\n"

    for item in tables:
        command += "\t" + item + "_ID INT DEFAULT NULL,\n"

    for item in fact_column_info:
        command += "\t" + item[0] + " " + item[1].upper() + " NOT NULL DEFAULT 0,\n"

    for item in dim_columns_unique:
        command += "\t" + item[0] + " " + item[1].upper() + ",\n"
    command = command[:-2]

    command += ");\n"

    return command


def insert_temp_fact(relationships, fact_columns, dim_columns):
    all_columns = fact_columns + dim_columns
    command = "INSERT INTO TEMP(\n"

    for item in all_columns:
        command += "\t" + item[0] + ",\n"

    command = command[:-2]
    command += ")\nSELECT\n"

    for item in all_columns:
        command += "\t" + item[2] + "." + item[0] + " " + item[0] + ",\n"
    command = command[:-2]
    tables = [i[2] for i in all_columns]
    tables = list(set(tables))

    command += "\nFROM "
    for item in tables:
        command += item + ", "
    command = command[:-2]

    relations = []
    for item in relationships:
        if item[1] in tables and item[4] in tables:
            relations.append(item)

    command += "\nWHERE\n"

    for item in relations:
        command += "\t" + item[1] + "." + item[2] + " = " + item[4] + "." + item[5] + " AND \n"
    command = command[:-6]
    command += "\nGROUP BY " + all_columns[0][0]

    return command


def update_tables(table, fact_columns):
    fact = [i[0] for i in factColumns]
    table_name = table[0][1]
    all_columns = []
    for item in table:
        if item[2] not in fact:
            all_columns.append(item[2])

    command = "UPDATE TEMP\nSET " + table_name + "_ID = dim." + table_name + ".SKey"

    command += "\nFROM dim." + table_name + "\nINNER JOIN TEMP ON "

    for item in all_columns:
        command += "\n\tTEMP." + item + " = " + "dim." + item + " AND"

    command = command[:-4]
    command += "\n\n"
    return command


def update_temp_fact(all_dim_tables, fact_columns):
    command = ""
    for item in all_dim_tables:
        command += update_tables(item, fact_columns)

    return command


def update_fact_table(columns):
    command = "INSERT INTO FACT_TABLE(\n"

    for item in columns:
        command += item + ", "

    command = command[:-2]
    command += ")\nSELECT\n"

    for item in columns:
        command += item + ", "
    command = command[:-2]
    command += "\nFROM TEMP\n"

    return command


if __name__ == '__main__':
    with open("AdventureWorks2016_attributes.csv") as csv_attributes:
        with open("AdventureWorks2016_relationships.csv") as csv_relationships:
            csv_reader_att = csv.reader(csv_attributes, delimiter="\t")
            csv_reader_rel = csv.reader(csv_relationships, delimiter="\t")

            attributes = list(csv_reader_att)[1:]  # schema/table/column/type/not_null/primary_key/unique
            relationships = list(csv_reader_rel)[1:]
            # schema_to/table_to/column_to/schema_from/table_from/column_from

            employee_list = []
            pay_history = []
            sales_person = []

            for items in attributes:
                if items[1] == "Employee":
                    employee_list.append(items)
                if items[1] == "EmployeePayHistory":
                    pay_history.append(items)
                if items[1] == "SalesPerson":
                    sales_person.append(items)

            factColumns = [["VacationHours", "", "Employee"], ["SickLeaveHours", "", "Employee"],
                           ["PayFrequency", "", "EmployeePayHistory"], ["Rate", "", "EmployeePayHistory"],
                           ["SalesLastYear", "", "SalesPerson"]]

            dim_tables = [employee_list, pay_history, sales_person]

            dimColumns = []
            for item in dim_tables:
                temp = scd1_create_table(item, factColumns)
                print(temp[0] + '\n')
                dimColumns += temp[1]

            fact_table_return = create_fact_table(employee_list + pay_history + sales_person, factColumns, "SCD1")

            print(fact_table_return[0] + '\n')

            print(create_temp_fact(employee_list + pay_history + sales_person, factColumns, dimColumns) + '\n')

            print(insert_temp_fact(relationships, factColumns, dimColumns) + '\n')

            for item in dim_tables:
                print(scd1_insert_row_temp(item, factColumns) + '\n')

            print(update_temp_fact(dim_tables, factColumns) + '\n')

            print(update_fact_table(fact_table_return[1] + [i[0] for i in factColumns]))
