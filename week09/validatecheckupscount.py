import re
import sys
from pyspark.sql import SparkSession 
from scmsanalysis.source import validateclaims as val

def main(argv,session):
       
    input_data = argv[0]
    output_data = argv[1]
   
    input_rdd = session.sparkContext.textFile(input_data)
    my_rdd = input_rdd.map(lambda line:val.parse(line))\
            .filter(lambda line:val.validate(line))\
            .filter(lambda line: line[7]=="G0402" or line[7]=="G0438" or line[7] == "G0439")\
            .repartition(1).persist()
    my_rdd.saveAsTextFile(output_data)        
            
    print(my_rdd.count())

    
    return

    
if __name__ == "__main__":
       
    spark = SparkSession.builder \
                       .appName('validateCheckupsCount') \
                       .master("local")\
                       .getOrCreate()
                        
    main(sys.argv[1:],spark)
    spark.stop()