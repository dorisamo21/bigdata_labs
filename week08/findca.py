# Import the PySpark libraries
from pyspark.sql import SparkSession 
import sys

def main(argv,session):

    input_data = argv[0]
    my_rdd = session.sparkContext.textFile(input_data)
    
#    my_split = my_rdd.map(lambda line: line.split("\t"))
#    my_filter = my_split.filter(lambda line: line[11] == 'CA')
#    my_count = my_filter.count()
    
    my_count= my_rdd.map(lambda line: line.split("\t")) \
                    .filter(lambda line: line[11] == 'CA') \
                    .count()
                    
    print("CA Medical Provider Count:",my_count)

if __name__ == "__main__":
       
    spark = SparkSession.builder \
                       .appName('findCA') \
                       .master("local")\
                       .getOrCreate()
                    
                    
# Configure the Spark context to give a name to the application
    
    main(sys.argv[1:],spark)
    spark.stop()
