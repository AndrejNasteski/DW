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

    for columns in att:
        if columns[2] == "ModifiedDate":  # we ignore the ModifiedDate column
            continue
        column_command = ""
        column_command += str(columns[2]) + " "
        column_command += str(columns[3]).upper()

        if columns[4] == "True":
            column_command += " NOT NULL"
        else:
            column_command += " NULL"

        if columns[6] == "True":
            column_command += " UNIQUE"

        column_command += ',\n'
        command += column_command

    command += "FirstLoadDate DATETIME NOT NULL,\n"
    command += "LastChangeDate DATETIME NOT NULL\n)"

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

    for columns in att:
        if columns[2] == "ModifiedDate":  # we ignore the ModifiedDate column
            continue
        column_command = ""
        column_command += str(columns[2]) + " "
        column_command += str(columns[3]).upper()

        if columns[4] == "True":
            column_command += " NOT NULL"
        else:
            column_command += " NULL"

        if columns[6] == "True":
            column_command += " UNIQUE"

        column_command += ',\n'
        command += column_command

    command += "IsCurrent BIT NOT NULL,\n"
    command += "IsDeleted BIT NULL,\n"
    command += "RowVersion INT NOT NULL,\n"
    command += "ValidFrom DATETIME NOT NULL,\n"
    command += "ValidTo DATETIME NULL\n)"

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
    for item in att:
        if item[2] == "ModifiedDate":
            continue
        if item[5] == "True":
            table_id_column = item[2]

        command += " odb." + item[2] + ","

    command += " GETDATE(), GETDATE()\n"
    command += "FROM " + database_name + "." + table_name + " AS odb\n"
    command += "LEFT JOIN dim." + att[1][1]\
               + "_SCD1 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column + "\n"
    command += "WHERE dwd.SKey IS NULL\n"

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
    for item in att:
        if item[2] == "ModifiedDate":
            continue
        if item[5] == "True":
            table_id_column = item[2]

        command += " odb." + item[2] + ","

    command += "1, 0, 1, GETDATE(), NULL\n"
    command += "FROM " + database_name + "." + table_name + " AS odb\n"
    command += "LEFT JOIN dim." + att[1][1] \
               + "_SCD1 AS dwd ON odb." + table_id_column + " = dwd." + table_id_column + "\n"
    command += "WHERE dwd.SKey IS NULL\n"

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
                if items[1] == "AWBuildVersion":
                    lista.append(items)

            print(scd1_create_table(lista))
            print("-------------------------")
            print(scd1_insert_row(lista, "OPERACIONA"))