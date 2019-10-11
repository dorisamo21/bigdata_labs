# Imports the PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

from pyspark.storagelevel import StorageLevel 
import sys
import os
import re


def main(argv,session):

    cms_data_in = argv[0]

    original_cms = session.sparkContext.textFile(cms_data_in)
    
    table_list = session.sql("show tables").collect()

#    call functions to generate data and build prov_treatment table
     
    selected_rdd = get_cms_base(original_cms)
    
    write_hive_table(selected_rdd)
    
    table_list = session.sql("show tables").collect()    
    for database, table, temporary in table_list:
        print('Tables:', table)
    
    execute_other_sql(session)
       
    return

def get_cms_base(original_cms):
    """
    This functions takes in Spark session and input file and
    generates a base rdd that has the data to build a hive table.
    @param session: spark interface
    @param cms_data_in: cms input file
    @return: return an rdd of rows, filtered, reformatted CMS_Data
    """

    filtered_cms = original_cms.map(lambda line:parse(line))\
                    .filter(lambda line:filter_cms(line[2]))\
                    .map(lambda line:get_treatment_content(line)).distinct()
    return filtered_cms

def parse(line):
    tokens = line.split('\t')    
    parsed_tuple = (tokens[0],tokens[11],tokens[16])
    return parsed_tuple

def filter_cms(hcpcs):
    filter_hcpcs = ['77385','77386','G6015','G6016','77387','G6001','G6002','G6017',
                    '77520','77521','77522','77523','77524','77525',
                    '77371','77372','77373','77422','77423',
                    '77770','77771','77772','77778','0394T','0395T',
                    '77761','77762','77763','77424','77425','77798',
                    '77014','77401','77402','77407','77412','77387','77750','G6003','G6004',
                    'G6005','G6006','G6007','G6008','G6009','G6010','G6011','G6012',
                    'G6013','G6014','90378','A9542','A9543','A9544','A9545','G3001',
                    'C9257','J9035','C9399','J3590','J0130','J0135','J0480','J0490',
                    'J0638','J0897','J1300','J1745','J2323','J2355','J2357','J2778',
                    'J3262','J3357','J9055','J9010','J9015','J9228','J9302','J9303',
                    'J9310','J9355']
    if hcpcs in filter_hcpcs:
        return True
    else:
        return False

def get_treatment_content(line):
    radiation_hcpcs = {'77385':'Modulated','77386':'Modulated','G6015':'Modulated','G6016':'Modulated',
                       '77387':'Image Guided','G6001':'Image Guided','G6002':'Image Guided','G6017':'Image Guided',
                       '77520':'Proton','77521':'Proton','77522':'Proton','77523':'Proton','77524':'Proton','77525':'Proton',
                       '77371':'Stereotactic','77372':'Stereotactic','77373':'Stereotactic',
                       '77422':'Neutron','77423':'Neutron',
                       '77770':'Brachytherapy','77771':'Brachytherapy','77772':'Brachytherapy','77778':'Brachytherapy','0394T':'Brachytherapy','0395T':'Brachytherapy',
                       '77761':'Intracavity','77762':'Intracavity','77763':'Intracavity',
                       '77424':'Intraoperative','77425':'Intraoperative',
                       '77798':'Surface',
                       '77014':'General','77401':'General','77402':'General','77407':'General','77412':'General','77387':'General','77750':'General','G6003':'General','G6004':'General',
                       'G6005':'General','G6006':'General','G6007':'General','G6008':'General','G6009':'General','G6010':'General','G6011':'General','G6012':'General',
                       'G6013':'General','G6014':'General'}  


    immunology_hcpcs = {'90378':'Synagis',
                        'A9542':'Synagis','A9543':'Synagis',
                        'A9544':'Bexxar','A9545':'Bexxar','G3001':'Bexxar',
                        'C9257':'Avastin','J9035':'Avastin',
                        'C9399':'Simponi','J3590':'Simponi',
                        'J0130':'ReoPro',
                        'J0135':'Humira',
                        'J0480':'Simulect',
                        'J0490':'Benlysta',
                        'J0638':'Ilaris',
                        'J0897':'Prolia',
                        'J1300':'Soliris',
                        'J1745':'Remicade',
                        'J2323':'Tysabri',
                        'J2355':'Neumega',
                        'J2357':'Xolair',
                        'J2778':'Lucentis',
                        'J3262':'Actemra',
                        'J3357':'Stelara',
                        'J9055':'Erbitux',
                        'J9010':'Campath',
                        'J9015':'Proleukin',
                        'J9228':'Yervoy',
                        'J9302':'Arzerra',
                        'J9303':'Vectibix',
                        'J9310':'Rituxan',
                        'J9355':'Herceptin'}

    if line[2] in radiation_hcpcs.keys():
        return line[0], line[1], "R", radiation_hcpcs.get(line[2])
    elif line[2] in immunology_hcpcs.keys():
        return line[0], line[1], "I", immunology_hcpcs.get(line[2])
        
def write_hive_table(selected_rdd):
    cmsDF = selected_rdd.map(lambda x:Row(state=x[1],npi=x[0],treatment_class=x[2],treatment_type=x[3]))\
                        .toDF()
    cmsDF.write.mode("overwrite").saveAsTable("default.prov_treatment")       
    print("created table prov_treatment")             
    return

def execute_other_sql(session):
    """
    This function can be used to execute sql statements on the hive table after it is created.
    This function must determine what to do with the data structure returned from sql i.e. print it.  
    @param session context used to execute SQL.
    @return: no return.
    """
    
    sql_stmnt1 = '''SELECT count(*) AS total_cnt
                    FROM prov_treatment'''

    sql_stmnt2 = '''SELECT npi,state,treatment_type 
                    FROM prov_treatment
                    limit 5'''
        
    prov_df = session.sql(sql_stmnt1)
    prov_df2 = session.sql(sql_stmnt2)

    print(prov_df.collect())

    for npi, state,treatment_type  in prov_df2.take(5):
        print(npi, state,treatment_type) 
    
    return 

if __name__ == "__main__":
       
    spark = SparkSession.builder \
                       .appName("BuildHiveTable") \
                       .master("local")\
                       .enableHiveSupport()\
                       .getOrCreate()
                    
                    
# Configure the Spark context to give a name to the application
    
    main(sys.argv[1:],spark)
    spark.stop()
