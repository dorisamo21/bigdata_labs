import re
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession 
from matplotlib import pyplot as plt

def main(argv,session):
       
    input_data = argv[0]
    pop_data= argv[1]
    out_data = argv[2]
   
    input_rdd = session.sparkContext.textFile(input_data)
    pop_rdd = session.sparkContext.textFile(pop_data)
    
    my_rdd = input_rdd.map(lambda line:parse(line)).filter(lambda line:filter_providers(line[1],line[2])).repartition(1)

    provider_schema = StructType([
        StructField("NPI",StringType(),False),
        StructField("ProviderType",StringType(),False),
        StructField("State",StringType(),False)
        ])

    provider_df = session.createDataFrame(my_rdd,schema=provider_schema)
    provider_df.createOrReplaceTempView("provider")

    my_population = pop_rdd.map(lambda line:parse_pop(line)).filter(lambda line:validate_senior_cnt(line[1])).repartition(1)
    senior_schema = StructType([
        StructField("State",StringType(),False),
        StructField("Population",StringType(),False)
        ])
    senior_df = session.createDataFrame(my_population,schema=senior_schema) 
    senior_df.createOrReplaceTempView("senior") 


    provider_cnt = spark.sql("SELECT State,ProviderType,Count(Distinct NPI) AS P_cnt \
                                FROM provider \
                                GROUP BY State,ProviderType")
    provider_cnt.createOrReplaceTempView("state_PG")
    
    ratio_provider_senior = spark.sql("SELECT A.State, B.ProviderType, A.Population AS seniors, B.P_cnt AS providers, (A.Population / B.P_cnt) AS Ratio \
                                        FROM senior A, state_PG B \
                                        WHERE A.State = B.State \
                                        ORDER BY Ratio").repartition(1).persist()

    ratio_provider_senior.show()
    ratio_provider_senior.write.format('com.databricks.spark.csv').option("header","true").option("delimiter","\t").save(out_data)                                
    generate_boxplot(ratio_provider_senior.toPandas())

    return



def parse(line):

    general_provider = ['FAMILY PRACTICE','PHYSICIAN ASSISTANT',
                        'NURSE PRACTITIONER','GENERAL PRACTICE',
                        'PREVENTIVE MEDICINE','GERIATRIC MEDICINE']
    
    mental_provider = ['PSYCHIATRY','CLINICAL PSYCHOLOGIST','GERIATRIC PSYCHIATRY',
                       'LICENSED CLINICAL SOCIAL WORKER','PSYCHOLOGIST (BILLING INDEPENDENTLY)',
                       'NEUROPSYCHIATRY','ADDICTION MEDICINE']
    
    tokens = line.split('\t')    
    providerType = tokens[13].upper()
    if providerType in general_provider:
        parsed_tuple = (tokens[0],"GP",tokens[11].upper())
        
    elif providerType in mental_provider:
        parsed_tuple = (tokens[0],"MHP",tokens[11].upper())

    else:
        parsed_tuple = (tokens[0],"else",tokens[11])
        
    return parsed_tuple

def filter_providers(ptype, state):
    states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL',
              'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT',
              'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
              'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
    
    if (ptype == "GP" or ptype == "MHP") and (state in states): # To filter Wrong States e.g. 'XX'
        return True
    else:
        return False
    


def parse_pop(line):
    tokens = line.split('\t') 
    population = tokens[5].replace("\"","").replace(",","")
    parsed_tuple = (tokens[0],population)
    return parsed_tuple

def validate_senior_cnt(token):  
    if re.match("^\\d+$",token):
        return True
    else:
        return False
    
def generate_boxplot(given_df):
    given_df.boxplot(column='Ratio', by='ProviderType',figsize=(5,5))
    plt.title("Ratio of Health Practitioners to Seniors by ProviderType")
    plt.xlabel("ProviderType")
    plt.ylabel("Ratio")
    plt.suptitle("")
    plt.show()
    
    
if __name__ == "__main__":
       
    spark = SparkSession.builder \
                       .appName('validateCheckupsCount') \
                       .master("local")\
                       .getOrCreate()
                        
    main(sys.argv[1:],spark)
    spark.stop()