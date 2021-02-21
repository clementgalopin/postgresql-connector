#!/usr/bin/env python
# -*- coding: utf-8 -*
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine
import traceback
import pandas as pd
import numpy as np

# Class to handle all operations statuses
class Operation(object):
    def __init__(self, status, msg, value=None, error=None):
        self.status = status
        self.msg = msg
        self.value = value
        self.error = error

# PostgreSQL connector class
class PostgreSQLConnector(object):
    # Class initialization
    def __init__(self, host, database, user, password):
        self.conn = None
        self.cur = None
        self.connected = False 
        self.logs = []
        self.config = {
            "host"      : host,
            "database"  : database,
            "user"      : user,
            "password"  : password
        }

    # Connection to the DB
    def connect(self):
        try:
            self.engine = create_engine("postgresql://%s:%s@%s/%s" % (self.config['user'], self.config['password'], self.config['host'], self.config['database']))
            self.conn = psycopg2.connect(**self.config)
            self.cur = self.conn.cursor()
            self.connected = True
            return Operation(status='success', msg='Connection has been established')
        except (Exception, psycopg2.DatabaseError) as error:
            return Operation(status='error', msg='Could not establish connection', error=error)

    # Checking if a table exists
    def table_exists(self, table_name, table_schema):
        try:
            query_str = "select * from information_schema.tables where table_name='%s' and table_schema='%s'" % (table_name, table_schema)
            self.cur.execute(query_str)
            exists = bool(self.cur.rowcount)
            return Operation(status='success', msg='Existence of table "%s"."%s" has been checked' % (table_name, table_schema), value=exists)
        except (Exception, psycopg2.DatabaseError) as error:
            return Operation(status='error', msg='Could not check existence of table "%s"."%s"' % (table_name, table_schema), error=error)

    # Getting a table's columns names
    def get_table_col_names(self, table_name, table_schema):
        col_names = []
        table_exists_operation = self.table_exists(table_name, table_schema)

        if table_exists_operation.status == 'success' and table_exists_operation.value == True:
            try:
                query_str = 'select * FROM "%s"."%s" LIMIT 0' % (table_schema, table_name)
                self.cur.execute(query_str)
                if self.cur.description:
                    col_names = [desc[0] for desc in self.cur.description]
                else:
                    return Operation(status='error', msg='Could not get the columns of table "%s"."%s"' % (table_name, table_schema), error='No description for this table.')
                return Operation(status='success', msg='Columns of table "%s"."%s" have been gotten' % (table_name, table_schema), value=col_names)
            except (Exception, psycopg2.DatabaseError) as error:
                return Operation(status='error', msg='Could not get the columns of table "%s"."%s"' % (table_name, table_schema), error=error)
        else:
            return Operation(status='error', msg='Could not get the columns of table "%s"."%s"' % (table_name, table_schema), error='The table does not exist.')

    # Batch insert function
    def batch_insert(self, table_name, table_schema, data, upsert=False, primary_keys=[]) :
        if upsert == True and len(primary_keys) == 0:
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='Upsert mode is activated but no primary key has been defined.')            

        # Testing if the data is a pandas DataFrame
        if not isinstance(data, pd.DataFrame):
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='The data is not a pandas DataFrame.')

        get_table_col_names = self.get_table_col_names(table_name, table_schema)
        table_col_names = get_table_col_names.value
        data_col_names = data.columns.tolist()
        # print(vars(get_table_col_names))
        # print(table_col_names)
        # print(data_col_names)

        # Getting the table list of columns
        if get_table_col_names.status != 'success' or not isinstance(table_col_names, list) or table_col_names == None:
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='There was an issue while checking the columns names.')

        # Checking if the primary_keys is a subset of the table list of columns
        if upsert == True and not any(table_col_names[pos:pos + len(primary_keys)] == primary_keys for pos in range(0, len(table_col_names) - len(primary_keys) + 1)):
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='Primary keys are not a subset of the table columns list.')

        # Checking if the data list of columns is a subset of the table list of columns
        if not any(table_col_names[pos:pos + len(data_col_names)] == data_col_names for pos in range(0, len(table_col_names) - len(data_col_names) + 1)):
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='The data columns list is not a subset of the table columns list.')

        # Building the query template
        try:
            query_col_list = sql.SQL(",").join(map(sql.Identifier, data_col_names))
            query_val_list = sql.SQL(",").join(map(sql.Placeholder, data_col_names))
            if upsert:
                query_pky_list  = sql.SQL(",").join(map(sql.Identifier, primary_keys))
                query = sql.SQL('INSERT INTO "%s"."%s" ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET ({}) = ({})' % (table_schema, table_name)).format(query_col_list, query_val_list, query_pky_list, query_col_list, query_val_list)
            else:
                query = sql.SQL('INSERT INTO "%s"."%s" ({}) VALUES ({})' % (table_schema, table_name)).format(query_col_list, query_val_list)
            # query_str = query.as_string(self.conn)
            # print(query_str)
        except:
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='Impossible to build the query template.')

        # Converting the data records into tuples which can be interpreted by the query template
        try:
            values = list(data.to_records(index=False))
        except:
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='Impossible to convert the data records into tuples.')

        # Executing the query for all the records of the data
        try:
            execute_batch(self.cur, query, values)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            # traceback.print_exc()
            return Operation(status='error', msg='Could dynamically insert the data into table "%s"."%s"' % (table_name, table_schema), error='Something went wrong while batch inserting: %s' % error)

        return Operation(status='success', msg='The data has been dynamically inserted into table "%s"."%s"' % (table_name, table_schema))

    # get dataframe from table  
    def table_to_df(self, table_name, table_schema):
        try:
            df = pd.read_sql_query('select * from "%s"."%s"' % (table_schema, table_name), con=self.engine)
            return Operation(status='success', msg='Df has been loaded from table "%s"."%s"' % (table_name, table_schema), value=df)
        except:
            return Operation(status='error', msg='Could not load df from table "%s"."%s"' % (table_name, table_schema))
        
    # get dataframe from table with restriction on date
    def table_to_df_by_date(self, table_name, table_schema, date_col, s_date=None, e_date=None):
        query = 'select * from "%s"."%s"' % (table_schema, table_name)
        if not date_col:
            return pd.DataFrame()        
        if s_date:
            query = query + ' WHERE "%s" >= \'%s\'' % (date_col, s_date)
        if e_date:
            query = query + ' AND "%s" <= \'%s\'' % (date_col, e_date)    
        
        try:
            return Operation(status='success', msg='Df has been loaded from table "%s"."%s"' % (table_name, table_schema), value=df)
        except:
            return Operation(status='error', msg='Could not load df from table "%s"."%s"' % (table_name, table_schema))
