from typing import Optional
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Integer, create_engine, ForeignKey 
from datetime import datetime
import invoice_connection_details as icd 
import os 
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame



def define_connection(dialect_and_driver, username, password, host, port, db_name:Optional[str]=icd.invoice_db_name):
    connection_string = f"{dialect_and_driver}://{username}:{password}@{host}:{port}/{db_name}"
    return connection_string

CONNECTION = define_connection(icd.conn_type, icd.username, icd.pwd, 
                               icd.hostname, icd.port_id, icd.invoice_db_name)
BASE = declarative_base()
ENGINE = create_engine(CONNECTION, echo=True)
SESSION = sessionmaker(ENGINE)
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
from spark_class import RoseSpark


class Invoice(BASE):
    '''we also want a col that is total duration that is a sum of duration...'''
    __tablename__ = f"alexander_patrie_prek_invoice_for_{icd.THIS_WEEK}"
    service_id = Column(Integer(), primary_key=True)
    service_date = Column(String())
    service_rendered = Column(String())
    duration = Column(Integer())
    

#class TotalInvoiceDuration(BASE):
#    __tablename__ = f"total_invoice_duration_for_{icd.THIS_WEEK}"
#    service_date = Column(Integer(),
#                    ForeignKey(f"alexander_patrie_prek_invoice_for_{icd.THIS_WEEK}"),
#                    primary_key=True)
#    total_duration = Column(Integer())
    
    
    
    
class Pencil(Invoice):
    def __init__(self, base, engine, new_invoice:Optional[bool]=False):
        session = sessionmaker(engine)
        self.local_session = session(bind=engine)
        if new_invoice:
            base.metadata.create_all(ENGINE)
    
    
    def prompt_batch_insert(self) -> list:         
        '''method that prompts user to add info to batch_list'''   
        batch = []
        amt_to_enter = int(input("How many services would you like to add?: "))
        if amt_to_enter > 0:
            for amt in range(amt_to_enter):
                service_date = input("service_date(mm/dd/yyyy): ")
                service_rendered = input("service rendered: ")
                duration = int(input("service_duration: "))
                new_invoice_info = (service_date,
                                    service_rendered,
                                    duration)
                batch.append(new_invoice_info)
        return batch
    
    
    def insert_batch(self, batch_info):
        '''insert n_dim batch into the db'''
        for row in batch_info:
            new_invoice = Invoice(service_id=row[0],
                                  service_date=row[1],
                                  service_rendered=row[2], 
                                  duration=row[3])
            self.local_session.add(new_invoice)
            self.local_session.commit()
            print('\n***new invoice added!!!***\n')
        
    
    def prompt_create_invoice(self, write_batch:Optional[bool]=False):
        '''will prompt the user for the amount of entries that they want to enter into
           the db'''
        batch = self.prompt_batch_insert()
        if write_batch:
            self.write_invoice_info_file(batch)
        return self.insert_batch(batch)
        
            
    def write_invoice_info_file(self, new_invoice_info:list):
        for rows in new_invoice_info:
            with open(f"{project_dir}/invoice_information.txt", "w") as f:
                f.write(str(rows))
                
####################END_PENCIL_CLASS####################################


def open_config(file_path: str) -> dict:
    if isinstance(file_path, str):
        return RoseSpark(config={}).open_file(file_path)

def spark_start(conf: dict) -> SparkSession:
    if isinstance(conf, dict):
        return RoseSpark(config={}).spark_start(conf)
    
def spark_stop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None
    
def read_in_from_pg(sql, conn) -> pd.DataFrame:
    return pd.read_sql(sql, conn)

def insert_invoice_batch(pencil:Pencil, batch_info):
    return pencil.insert_batch(batch_info)

def write_new_invoice_file(pencil:Pencil, new_invoice_info):
    return pencil.write_invoice_info_file(new_invoice_info)
        
        
if __name__ == "__main__":
    conf = open_config(f"{project_dir}/config/config.json")
    rose = spark_start(conf)
    pencil = Pencil(BASE, ENGINE, new_invoice=True)
    wk0_invoice = [[0, "10/03/2022", "Music Class - PREK", 30],
                   [1, "10/03/2022", "Music Class - TODDLER", 30],
                   [2, "10/06/2022", "Music Class - PREK", 30],
                   [3, "10/06/2022", "Music Class - TODDLER", 30]]
    insert_invoice_batch(pencil=pencil, 
                         batch_info=wk0_invoice)
    write_new_invoice_file(pencil, wk0_invoice)
    #pencil.prompt_create_invoice(write_batch=True)
    df0 = read_in_from_pg(f"SELECT * FROM alexander_patrie_prek_invoice_for_{icd.THIS_WEEK}",
                          CONNECTION)
    print(df0.head())
    df0.to_excel(f"{project_dir}/{icd.THIS_WEEK}.xls", index=None, header=True)
    
    #df0.createOrReplaceTempView("d")
    #df1 = rose.sql("select ")
    
    