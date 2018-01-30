from collections import defaultdict
import os
import re
import csv
import sys
import sqlparse
import pandas as pd
# Add vendor directory to module search path
parent_dir = os.path.abspath(os.path.dirname(__file__))
vendor_dir = os.path.join(parent_dir, 'vendor')

sys.path.append(vendor_dir)
import moz_sql_parser

class SQLEngine(object):
    def __init__(self):
        self.tables = set()
        self.metadata = {} 
        self.data = {}
        self.querystring = ""
        self.selectArgs = {}
        self.fromArgs = {}
        self.whereArgs = {}
        self.joinedColumns = {}
        self.joinedTables = []
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
                i += 1
                col = lines[i].rstrip("\r\n")
                while col != "<end_table>" :
                    self.metadata[table].append(col)
                    print self.metadata[table]
                    i += 1
                    col = lines[i].rstrip("\r\n")

                i += 1
        return "SUCCESS"

    def initialize_data(self):
        print self.tables
        for table in self.tables:
            file = table+'.csv'
            df = pd.read_csv(file,header = None,names = list(self.metadata[table]))
            self.data[table] = df

    def check_query(self, query):
        self.querystring = query
        print self.querystring
        string = re.match(r"select(.*)from(.*)", self.querystring)
        print string
        if not string:
            self.showError()
            return
        string = self.querystring
        string_param = string.replace(',',' ').split()
        index_select = self.find_string(string_param, "select")
        index_from = self.find_string(string_param, "from")
        index_where = self.find_string(string_param, "where")
        self.selectArgs = string_param[index_select+1:index_from]
        self.fromArgs = string_param[index_from+1:index_where]
        self.whereArgs = string_param[index_where+1:]
        if "where" in self.querystring:    
            string = re.match(r"select(.*)from(.*)where(.*)", self.querystring)
            if not string:
                self.showError()
                return
        return


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

        
if __name__ == "__main__":
    print "WELCOME TO MINI-SQL ENGINE"
    database = SQLEngine()
    database.initialize_metadata("metadata.txt")
    database.initialize_data()
    print database.metadata
    print database.data
    while True :
        database.error = False
        queryString = raw_input("SQL> ")
        if queryString == "exit" :
            print "Bye"
            break
        else:
            queryString = queryString[:-1]
            database.check_query(queryString)
            print database.selectArgs
            print database.fromArgs
            print database.whereArgs
            
    