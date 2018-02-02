#!/usr/bin/env python

"""main.py: Run mini-sql engine, takes argument command supported by mysql"""

__author__      = "Satyam Mittal"
__copyright__   = "Copyright 2018"

from collections import defaultdict
import os
import re
import csv
import sys
import sqlparse
import pandas as pd
import json
# Add vendor directory to module search path
parent_dir = os.path.abspath(os.path.dirname(__file__))
vendor_dir = os.path.join(parent_dir, 'vendor')

sys.path.append(vendor_dir)
import moz_sql_parser as moz

class SQLEngine(object):
    def __init__(self):
        self.tables = set()
        self.metadata = {} 
        self.columns = {}
        self.data = {}
        self.querystring = ""
        self.selectArgs = {}
        self.fromArgs = {}
        self.whereArgs = {}
        self.distinct = False
        self.joinedColumns = []
        self.joinedTables = []
        self.int_flag= True
        self.error = False


    def initialize_metadata(self, metadata):
        metadata = open(metadata, "r")
        lines = metadata.readlines()
        numlines = len(lines)
        i = 0
        while i < numlines :
            line = lines[i].rstrip("\r\n")
            if line == "<begin_table>" :
                i += 1
                table = lines[i].rstrip("\r\n")
                self.tables.add(table)
                self.metadata[table] = list()
                self.columns[table] = list()
                i += 1
                col = lines[i].rstrip("\r\n")
                while col != "<end_table>" :
                    self.metadata[table].append(col)
                    self.columns[table].append(table+':'+col)
                    i += 1
                    col = lines[i].rstrip("\r\n")

                i += 1
        return "SUCCESS"

    def initialize_data(self):
        for table in self.tables:
            file = table+'.csv'
            df = pd.read_csv(file,header = None,names = list(self.metadata[table]))
            self.data[table] = df
            


    def check_query(self, query):
        self.querystring = query
        try:
            parsed_query = moz.parse(self.querystring)
        except Exception as e:
            #self.showError()
            return
        query = parsed_query
        self.selectArgs = query.get('select', {})
        self.fromArgs = query.get('from', {})
        self.fromtoArr()
        self.selecttoArr()
        self.whereArgs = query.get('where', {})
        return
    
    def fromtoArr(self):
        if not isinstance(self.fromArgs, list):
            y = list()
            y.append(str(self.fromArgs))
            self.fromArgs = y
        return


    def check_aggregate(self,array,dct_df):
        for key in array.keys():
            col = array[key]
            tab = self.return_column_name(col)
            res = dct_df[tab]
            if key == "distinct":
                return key
            print key+"("+tab+")"
            if "avg" in key:
                print res.agg('mean')
            else:
                print res.agg(key)

        return "AGG"

    def selecttoArr(self):
        if not isinstance(self.selectArgs, list):
            y = list()
            if not isinstance(self.selectArgs, dict):
                y.append(str(self.selectArgs))
            elif not isinstance(self.selectArgs["value"], dict):
                y.append(str(self.selectArgs["value"]))
            else:
                y.append(self.selectArgs["value"])
            self.selectArgs = y
        else:
            y=list()
            for x in self.selectArgs:
                if not isinstance(x, dict):
                    y.append(str(x))
                elif not isinstance(x["value"],dict):
                    y.append(str(x["value"]))
                else:
                    y.append(x["value"])
        self.selectArgs = y
            

    def apply_from(self):
        result = pd.DataFrame()
        for table in self.fromArgs:
            df = self.data[table].copy()
            df.columns = [table + '.' + str(col) for col in df.columns]
            df["join:key"]=1
            if result.empty:
                result = df
            else: 
                result = pd.merge(result, df)
        try:
            result = result.drop(["join:key"], axis=1)
        except Exception as e:
            pass
        self.joinedTables = result
    
    def return_column_name(self, name):
        try:
            if '.' in name:
                array = name.split('.')
                table_data = self.metadata[array[0]]
                if array[1] in table_data:
                    return str(array[0]) + '.' + str(array[1])
        except Exception as e:
            pass
        for key in self.fromArgs:
            col_list = self.metadata[key]
            for column in col_list:
                if column == name:
                    return (str(key) + '.' + str(name))
        return self.get_compare_literal(name)
    
    def get_compare_literal(self, value):
        try:
            literal = value['literal']
            return str(literal)
        except Exception as e:
            try:
                return float(value)
            except Exception as e:
                return 'ERROR'
    def return_copy_dataframe(self, df):
        result=df.copy()
        return result

    def apply_condition(self, dict, head_table):
        if dict==None or len(dict)==0:
            return head_table
        for key in dict.keys():
            #print key
            if key=="eq" or key=="lt" or key=="gt" or key=="lte" or key=="gte" or key=="neq":
                eq = dict[key]
                key = str(key)
                if len(eq) == 2:
                    result= self.return_column_name(eq[0])
                    comp1=result
                    df = head_table
                    comp2 = self.return_column_name(eq[1])
                    #print comp2, comp1
                    if not isinstance(comp2, str):
                        if key=="eq":
                            head_table=df[df[comp1]==int(comp2)]
                        elif key=="lt":
                            head_table=df[df[comp1]<int(comp2)]
                        elif key=="gt":
                            head_table=df[df[comp1]>int(comp2)]
                        elif key=="lte":
                            head_table=df[df[comp1]<=int(comp2)]
                        elif key=="gte":
                            head_table=df[df[comp1]>=int(comp2)]
                        else:
                            head_table=df[df[comp1]!=int(comp2)]
                    else:
                        if key=="eq":
                            self.joinedColumns.append(comp2)
                            head_table=df[df[comp1]==df[comp2]]
                        elif key=="lt":
                            head_table=df[df[comp1]<df[comp2]]
                        elif key=="gt":
                            head_table=df[df[comp1]>df[comp2]]
                        elif key=="lte":
                            head_table=df[df[comp1]<=df[comp2]]
                        elif key=="gte":
                            head_table=df[df[comp1]>=df[comp2]]
                        else:
                            head_table=df[df[comp1]!=df[comp2]]
                del df
                return head_table
            elif key=="and" or key=="or":
                eq = self.whereArgs[key]
                table1 = self.return_copy_dataframe(head_table)
                df1 = self.apply_condition(eq[0], table1)
                table2 = self.return_copy_dataframe(head_table)
                df2 = self.apply_condition(eq[1], table2)
                result = {}
                if key=="and":
                    result=pd.merge(df1, df2, how='inner')
                else:
                    result=pd.merge(df1, df2, how='outer')
                return result

    def select_columns_for_table(self, coln_list):
        val = list()
        for x in coln_list:
            col_ret = self.return_column_name(x)
            if col_ret != 'ERR0R' and col_ret not in self.joinedColumns:
                val.append(col_ret)
        return val

    def select(self, df):
        final_list = list()
        sel = True
        if '*' not in self.selectArgs:
            array = self.selectArgs[0]
            aggregate = "no"
            if isinstance(array, dict):
                aggregate = self.check_aggregate(array, df)
            if aggregate=="AGG":
                return "AGG"
            elif aggregate=="distinct":
                self.distinct = True
                self.selectArgs[0]["value"]=self.selectArgs[0]["distinct"]
                del self.selectArgs[0]["distinct"]
                self.selecttoArr()
            col_list = self.select_columns_for_table(self.selectArgs)
            final_list.extend(col_list)
        else:
            for coln in self.joinedColumns:
                if coln in df.columns:
                    df = df.drop(coln, axis=1) 
            table_data = df
            sel = False
        if sel:
            table_data = df[final_list] 
        try:
            table_data = table_data.dropna(axis=0, how='all')
            table_data = table_data.dropna(axis=1, how='all')
        except Exception as e:
            pass
        return table_data

    def find_string(self, list_str, string):
        index=0
        for elem in list_str:
            if elem == string:
                return index
            index+=1
        return index

    def showError(self) :
    	self.error = True
    	print "Error in query syntax"
    
    def clear(self):
        del self.data
        del self.metadata
        del self.columns
    #def select 

        
if __name__ == "__main__":
    #print "WELCOME TO MINI-SQL ENGINE"
    database = SQLEngine()
    database.initialize_metadata("metadata.txt")
    database.initialize_data()
    database.error = False
    #queryString = raw_input("SQL> ")
    queryString = sys.argv[1]
    #print queryString
    if queryString[-1]==';':
        try:
            queryString = queryString[:-1]
            database.check_query(queryString)
            database.apply_from()
            df = database.joinedTables.copy()
            result= database.apply_condition(database.whereArgs,df)
            output= database.select(result)
            if not isinstance(output, str) and output.empty:
                print "Empty Set"
            elif not isinstance(output, str):
                if database.distinct:
                    if database.int_flag:
                        try:
                            output = output.astype(int)
                        except Exception as e:
                            pass
                    output = output.drop_duplicates()
                print output.to_csv(sep=',',index=False, line_terminator='\n')[:-1]
        except Exception as e:
            database.showError()
        
    else:
        database.showError()
        
    database.clear()
    