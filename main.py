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
        self.joinedColumns = {}
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
                    #print self.metadata[table]
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
            #print query
            parsed_query = moz.parse(self.querystring)
            #print parsed_query
        except Exception as e:
            #print e
            self.showError()
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
            #print tab[1],tab[0]
            res = dct_df[tab[1]][tab[0]]
            if "avg" in key:
                print res.agg('mean')
            elif key == "distinct":
                return key
            else:
                print res.agg(key)

        #print array
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
                #print type(x)
                if not isinstance(x, dict):
                    y.append(str(x))
                elif not isinstance(x["value"],dict):
                    y.append(str(x["value"]))
                else:
                    y.append(x["value"])
        self.selectArgs = y
            

    def apply_from(self):
        df = pd.DataFrame()
        for table in self.fromArgs:
            df = df.join(self.data[table], how="outer")
        #print self.data["table1"].join(self.data["table2"], lsuffix='_caller', rsuffix='_other')
        self.joinedTables = df
    
    def return_column_name(self, name):
        try:
            if '.' in name:
                array = name.split('.')
                table_data = self.metadata[array[0]]
                if array[1] in table_data:
                    #ol_name = array[0]+':'+array[1]
                    return (str(array[1]), str(array[0]))
        except Exception as e:
            pass
        for key in self.fromArgs:
            col_list = self.metadata[key]
            #print col_list
            for column in col_list:
                #print column
                if column == name:
                    return (str(name), str(key))
        return self.get_compare_literal(name)
    
    def get_compare_literal(self, value):
        #print value
        try:
            literal = value['literal']
            return str(literal)
        except Exception as e:
            try:
                return float(value)
            except Exception as e:
                return ("ERROR", "ERROR")
    def return_copy_dataframe(self, df):
        result = {}
        for t in df:
            result[t]=df[t].copy()
        return result

    def apply_condition(self, dict, head_table):
        if dict==None or len(dict)==0:
            return head_table
        for key in dict.keys():
            #print key
            if key=="eq" or key=="lt" or key=="gt" or key=="lte" or key=="gte" or key=="neq":
                eq = dict[key]
                key = str(key)
                #print key
                #print len(eq)
                if len(eq) == 2:
                    #print eq[0]
                    result= self.return_column_name(eq[0])
                    comp1=result[0]
                    table=result[1]
                    #print head_table
                    df = head_table[result[1]]
                    #print df
                    #print comp1, table
                    comp2 = self.return_column_name(eq[1])
                    #print comp2
                    if not isinstance(comp2, tuple) or comp2[0]=="ERROR":
                        #print comp1, comp2
                        #print df
                        #print "assd"
                        #print int(comp2)
                        if key=="eq":
                            head_table[table]=df[df[comp1]==int(comp2)]
                        elif key=="lt":
                            head_table[table]=df[df[comp1]<int(comp2)]
                        elif key=="gt":
                            head_table[table]=df[df[comp1]>int(comp2)]
                        elif key=="lte":
                            head_table[table]=df[df[comp1]<=int(comp2)]
                        elif key=="gte":
                            head_table[table]=df[df[comp1]>=int(comp2)]
                        else:
                            head_table[table]=df[df[comp1]!=int(comp2)]
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
                    for a, b in zip(df1, df2):
                        y1 = df1[a]
                        y2 = df2[b]
                        result[a]=pd.merge(y1, y2, how='inner')
                else:
                    for a, b in zip(df1, df2):
                        y1 = df1[a]
                        y2 = df2[b]
                        result[a]=pd.merge(y1, y2, how='outer')
                return result

    def select_columns_for_table(self, table, coln_list):
        val = list()
        for x in coln_list:
            #print type(x)
            col_ret = self.return_column_name(x)
            if col_ret[1] == table:
                val.append(col_ret[0])
        return val

    def select(self, df):
        result = pd.DataFrame()
        #print self.selectArgs
        for table in self.fromArgs:
            #print df[table]
            #print self.selectArgs
            if '*' not in self.selectArgs:
                array = self.selectArgs[0]
                #print array
                aggregate = "no"
                if isinstance(array, dict):
                    aggregate = self.check_aggregate(array, df)
                #print type(array)
                if aggregate=="AGG":
                    return result
                elif aggregate=="distinct":
                    self.distinct = True
                    self.selectArgs[0]["value"]=self.selectArgs[0]["distinct"]
                    del self.selectArgs[0]["distinct"]
                    self.selecttoArr()
                #print self.selectArgs
                col_list = self.select_columns_for_table(table, self.selectArgs)
                table_data = df[table][col_list]
                table_data.columns = [table + '.' + str(col) for col in table_data.columns]
                result = result.join(table_data, how="outer")
            else:
                table_data = df[table]
                table_data.columns = [table + '.' + str(col) for col in table_data.columns]
                result = result.join(table_data, how="outer")
        result = result.dropna(axis=0, how='all')
        result = result.dropna(axis=1, how='all')
        return result

    def find_string(self, list_str, string):
        index=0
        for elem in list_str:
            if elem == string:
                return index
            index+=1
        return index

    def showError(self) :
    	self.error = True
    	print "Error in query syntax - Not an SQL query"
    
    def clear(self):
        del self.data
        del self.metadata
        del self.columns
    #def select 

        
if __name__ == "__main__":
    print "WELCOME TO MINI-SQL ENGINE"
    #print database.metadata
    #print database.data
    while True :
        database = SQLEngine()
        database.initialize_metadata("metadata.txt")
        database.initialize_data()
        database.error = False
        #queryString = raw_input("SQL> ")
        queryString = "select DISTINCT table1.B, table2.B from table1, table2 where A>'-1000' or C>5500;"
        if queryString == "exit" :
            print "Bye"
            break
        elif queryString[-1]==';':
            queryString = queryString[:-1]
            database.check_query(queryString)
            #print database.selectArgs
            #print database.fromArgs
            #print database.whereArgs
            #database.apply_from()
            #print database.joinedTables
            df = database.data.copy()
            #print database.selectArgs
            result= database.apply_condition(database.whereArgs,df)
            output= database.select(result)
            if not output.empty:
                if database.distinct:
                    if database.int_flag:
                        try:
                            output = output.astype(int)
                        except Exception as e:
                            pass
                    output = output.drop_duplicates()
                print output
                #print output.astype(int)
                print output.to_csv(sep=',',index=False, line_terminator='\n')[:-1]
            
        else:
            database.showError()
        break
            
    database.clear()
    