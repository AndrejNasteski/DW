import csv


def scd1_create_table(att):
    """
    :param att:
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

    command = "CREATE TABLE dim." + att[1][1] + "_SCD1\n" + \
              "(SKey BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL,\n"
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = ["ModifiedDate"]  # filter column names that should not be in the dimension table

    for columns in att:
        if columns[2] in column_names and columns[5] == "False":
            continue
        if columns[3] in data_types and columns[5] == "False":
            continue
        column_command = "" \
                         + str(columns[2]) + " " \
                         + str(columns[3]).upper()
        if columns[4] == "True":
            column_command += " NOT NULL"
        else:
            column_command += " NULL"
        if columns[6] == "True":
            column_command += " UNIQUE"
        column_command += ',\n'
        command += column_command

    command += "FirstLoadDate DATETIME NOT NULL,\n" \
               + "LastChangeDate DATETIME NOT NULL\n)"

    return command


def scd2_create_table(att):
    """
    :param att:
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

    command = "CREATE TABLE dim." + att[1][1] + "_SCD2\n" + \
              "(SKey BIGINT IDENTITY(1,1) NOT NULL,\n"
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = ["ModifiedDate"]  # filter column names that should not be in the dimension table

    for columns in att:
        if columns[2] in column_names and columns[5] == "False":
            continue
        if columns[3] in data_types and columns[5] == "False":
            continue
        column_command = "" \
                         + str(columns[2]) + " " \
                         + str(columns[3]).upper()
        if columns[4] == "True":
            column_command += " NOT NULL"
        else:
            column_command += " NULL"
        if columns[6] == "True":
            column_command += " UNIQUE"
        column_command += ',\n'
        command += column_command

    command += "IsCurrent BIT NOT NULL,\n" \
               + "IsDeleted BIT NULL,\n" \
               + "RowVersion INT NOT NULL,\n" \
               + "ValidFrom DATETIME NOT NULL,\n" \
               + "ValidTo DATETIME NULL\n)"

    return command


def scd1_insert_row(att, database_name):
    """

    :param database_name: Name of your operational database
    :param att: list of all rows with the same table name
    [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:
        INSERT INTO dim.[table]_SCD1
    SELECT odb.[attributes], GETDATE(), GETDATE()
    FROM [database_name].[schema].[table] AS odb
    LEFT JOIN dim.[table_SCD1] AS dwd ON odb.[table]ID = dwd.[table]ID
    WHERE dwd.SKey IS NULL

    """

    table_name = att[1][1]
    command = "INSERT INTO dim." + table_name + "_SCD1\nSELECT"
    table_id_column = att[1][2] + "ID"  # the first column is the default value for table primary key
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = ["ModifiedDate"]  # filter column names that should not be in the dimension table

    for item in att:
        if item[2] in column_names and item[5] == "False":
            continue
        if item[3] in data_types and item[5] == "False":
            continue
        if item[5] == "True":
            table_id_column = item[2]

        command += " odb." + item[2] + ","

    command += " GETDATE(), GETDATE()\n" \
               + "FROM " + database_name + "." + table_name + " AS odb\n" \
               + "LEFT JOIN dim." + att[1][1] \
               + "_SCD1 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column + "\n" \
               + "WHERE dwd.SKey IS NULL\n"

    return command


def scd2_insert_row(att, database_name):
    """

    :param database_name: Name of your operational database
    :param att: list of all rows with the same table name
    [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:
    INSERT INTO dim.[table]_SCD2
    SELECT odb.[attributes], 1, 0, 1, GETDATE(), NULL
    FROM [database_name].[schema].[table] AS odb
    LEFT JOIN dim.[table]_SCD2 AS dwd ON d.[table]ID = dwd.[table]ID
    WHERE dwd.SKey IS NULL

    """
    table_name = att[1][1]
    command = "INSERT INTO dim." + table_name + "_SCD2\nSELECT"
    table_id_column = att[1][2] + "ID"  # the first column is the default value for table primary key
    data_types = list()  # filter data types that should not be in the dimension table
    column_names = ["ModifiedDate"]  # filter column names that should not be in the dimension table

    for item in att:
        if item[2] in column_names and item[5] == "False":
            continue
        if item[3] in data_types and item[5] == "False":
            continue
        if item[5] == "True":
            table_id_column = item[2]

        command += " odb." + item[2] + ","

    command += "1, 0, 1, GETDATE(), NULL\n" \
               + "FROM " + database_name + "." + table_name + " AS odb\n" \
               + "LEFT JOIN dim." + att[1][1] \
               + "_SCD1 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column + "\n" \
               + "WHERE dwd.SKey IS NULL\n"

    return command


def scd1_update_row(att):
    """

    :param att: list of all rows with the same table name
    [[schema, table, column1, type, not_null, primary_key, unique],
      [schema, table, column2, type, not_null, primary_key, unique], ...]
    :return:

    UPDATE dim.Department_SCD1
    SET Name=odb.Name, GroupName=odb.GroupName, DepartmentId=odb.DepartmentID
    FROM odb.HumanResources.Department AS odb
    WHERE dim.Department_SCD1.DepartmentID=odb.DepartmentID
    AND HASH(old_fields) != HASH(new_fields)
    """

    table_name = att[1][1]
    command = "UPDATE dim." + table_name + "_SCD1\nSET "

    table_id_column = att[1][2] + "ID"  # the first column is the default value for table primary key
    old_fields = ""
    new_fields = ""
    for item in att:
        if item[5] == "True":
            table_id_column = item[2]
        old_fields += "dim." + table_name + "_SCD1." + item[2] + ", "
        new_fields += "odb." + item[2] + ", "
        command += item[2] + " = odb." + item[2] + ", "

    old_fields = old_fields[:-2]  # remove excess characters
    new_fields = new_fields[:-2]  # remove excess characters
    command = command[:-2]  # remove excess characters

    command += "\nFROM odb." + att[0][0] + "." + table_name + " AS odb\n" \
               + "WHERE dim." + table_name + "_SCD1." + table_id_column + " = odb." + table_id_column \
               + "\nAND HASH(" + old_fields + ") != \n HASH(" + new_fields + ")"

    return command


def scd2_update_row(att):
    """

        :param att: list of all rows with the same table name
        [[schema, table, column1, type, not_null, primary_key, unique],
          [schema, table, column2, type, not_null, primary_key, unique], ...]
        :return:

        UPDATE dim.Department_SCD1
        SET Name=odb.Name, GroupName=odb.GroupName, DepartmentId=odb.DepartmentID
        FROM odb.HumanResources.Department AS odb
        WHERE dim.Department_SCD1.DepartmentID=odb.DepartmentID
        AND HASH(old_fields) != HASH(new_fields)
        """

    table_name = att[1][1]
    table_id_column = ""
    old_fields = ""
    new_fields = ""
    command = "IF object_id('#SCD2') IS NOT NULL\nDROP TABLE #SCD2\n\n"
    command += "SELECT dwd.SKey, dwd.RowVersion + 1 as NewVersion,\n" \
               "\tCAST(1 AS BIT) AS NewIsCurrent,\n" \
               "\tGETDATE() AS NewValidFrom,\n" \
               "\tCAST(NULL AS DATETIME) AS NewValidTo,\n"

    for item in att:
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
               "FROM odb." + att[0][0] + '.' + table_name + " AS odb\n" \
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


if __name__ == '__main__':
    with open("AdventureWorks2016_attributes.csv") as csv_attributes:
        with open("AdventureWorks2016_relationships.csv") as csv_relationships:
            csv_reader_att = csv.reader(csv_attributes, delimiter="\t")
            csv_reader_rel = csv.reader(csv_relationships, delimiter="\t")

            attributes = list(csv_reader_att)[1:]  # shema/table/column/type/not_null/primary_key/unique
            relationships = list(csv_reader_rel)[1:]
            # shema_to/table_to/columnt_to/shema_from/table_from/column_from

            tables = list()
            for entry in attributes:
                if entry[1] not in tables:
                    tables.append(entry[1])

            # print(attributes)

            lista = list()

            # print(tables)

            for items in attributes:
                # print(items[1])
                if items[1] == "Department":
                    lista.append(items)

            print(scd1_update_row(lista))
            print("-------------------------")
            print(scd2_update_row(lista))
            # print(scd1_insert_row(lista, "OPERACIONA"))
