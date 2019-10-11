# Import the PySpark libraries
from pyspark.sql import SparkSession 
import sys

def main(argv,session):

    input_data = argv[0]
    output_data = argv[1]
    input_rdd = session.sparkContext.textFile(input_data)

    my_rdd = input_rdd.map(lambda line:line.split("\t"))\
            .filter(lambda line: line[1].upper().startswith("A")).repartition(1).persist()
            
    print(my_rdd.count())
    my_rdd.saveAsTextFile(output_data)

if __name__ == "__main__":
       
    spark = SparkSession.builder \
                       .appName('findNamesStartsWithA') \
                       .master("local")\
                       .getOrCreate()
                    
                    
# Configure the Spark context to give a name to the application
    
    main(sys.argv[1:],spark)
    spark.stop()
