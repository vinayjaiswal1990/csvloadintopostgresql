#!/usr/bin/python


import glob,os,sys
import psycopg2
import psycopg2.extras
import logging
import pandas as pd


class google_drive_migration:
    logging.basicConfig(filename='/Users/vinayj.aiswal/Downloads/etl.log',level=logging.DEBUG,format='%(asctime)s %(name)s %(message)s ',datefmt='%Y-%m-%d %H:%M:%S',filemode='a')


# Constructor class , variables initialize.

    def __init__(self,drive_path,table_name):

        self.drive_path= drive_path
        self.table_name = table_name

# this method is use for Searching filesinventory csv files.

    def Search_FileInventoryCsv(self):

        fileinventorycsv_list = []

        for DirName in os.listdir(self.drive_path):
            path = os.path.join(self.drive_path,DirName)
            os.chdir(path)
            for file in glob.glob("*FileInventory*"):
                files = os.path.join(path,file)
                fileinventorycsv_list.append(files)
        return fileinventorycsv_list

# this method is use to print the count of fileinventory csv files.

    def FileInventorycsvCount(self):
        csvfiles = self.Search_FileInventoryCsv()
        Filecount=len(csvfiles)
        logging.info("Total FileInventory csv file count from this path %s = %s \n"%(self.drive_path,Filecount))

# this method is use for counting total rows in all fileinventory csv files from target dir .

    def totalrowscount(self):
        sum =0
        csvfiles = self.Search_FileInventoryCsv()
        for i in csvfiles:
            a=len(pd.read_csv(i))
            sum = sum +a
        logging.info("Total rows count of fileinventorycsv: %s\n"%(sum))


# this method is use to do the SQL query formation on the basis fileinventory csv files path.

    def SqlQuery_Formation(self):
        csvfiles = self.Search_FileInventoryCsv()
        Sql_list = []
        for csvfile in csvfiles:
            SQL = "COPY {} from '{}' CSV HEADER; ".format(self.table_name,csvfile)
            Sql_list.append(SQL)
        logging.info("This is list of SQL Query for COPY command \n")
        logging.info(Sql_list)
        logging.info("\n")
        return Sql_list

# this method is use for Postgresql connection check.

    def postgresql_connection(self):
         try:
             conn = psycopg2.connect(dbname="gdm",user="postgres",password="")
             cur = conn.cursor()
             cur.execute("select version()")
             logging.info("Database connection opened Successfully")
             logging.info(cur.fetchone())
             logging.info("\n")
             return cur , conn

         except Exception as e:
             logging.error("Error: {}".format(str(e)))
             sys.exit(1)

# This method is use loading csv files data into DB tables .

    def csv_file_load(self):
        sql_query_list = self.SqlQuery_Formation()
        cur,conn  = self.postgresql_connection()
        for i in sql_query_list:
            cur.execute(i)
            a=cur.statusmessage
            conn.commit()
            logging.info(a)
            cur.execute("select count(*) from google_inventory")
            logging.info(cur.fetchone())
        conn.close()
        logging.info("\n")
        logging.info("Connection Closed Successfully")


# Arguments pass and object creation and method call

one_drive = google_drive_migration("/Users/vinayj.aiswal/Downloads/google_drive/","google_inventory")
one_drive.FileInventorycsvCount()
one_drive.totalrowscount()
one_drive.csv_file_load()

