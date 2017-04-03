__author__ = 'Hari, Gnanaprakasam'

from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os.path
import atexit


####################################################################################################
# Basic Spark transformations
# Class: Mod_BabyNames
# Method(s):get_first_column, filteing_names, parallelization
#
####################################################################################################




class Mod_BabyNames():

    FILE_NAME = "Baby_Names__Beginning_2007.csv"
    file_relative_path = os.path.dirname(__file__) + "/" + FILE_NAME
    sparksc = SparkSession.builder.master("local").appName("BabyNames").getOrCreate()
    getName = sparksc.sparkContext.textFile(file_relative_path)

    def spark_stop(self):
        print("*******Closing All Spark sessions****************")
        atexit.register(self.sparksc.stop)

    def get_first_column(self):
        row_names = self.getName.map(lambda line: line.split(","))
        # for row in row_names.take(row_names.count()):
        #  print(row[1])
        return row_names


    def filter_names(self):
        row_names = self.getName.map(lambda line: line.split(","))
        print(row_names.filter(lambda line: "JACKSON" in line).collect())


    def rdd_parallel_union(self):
        rdd_set_one = self.sparksc.sparkContext.parallelize(range(1,10))
        rdd_set_two=self.sparksc.sparkContext.parallelize(range(1,20))
        rdd_op=rdd_set_one.union(rdd_set_two).collect()
        print(rdd_op)


    def rdd_parallel_intersect(self):
        rdd_set_one = self.sparksc.sparkContext.parallelize(range(1,10))
        rdd_set_two=self.sparksc.sparkContext.parallelize(range(1,20))
        rdd_op=rdd_set_one.intersection(rdd_set_two).collect()
        print(rdd_op)

    def rdd_parallel_distinct(self):
        rdd_set_one = self.sparksc.sparkContext.parallelize(range(1,10))
        rdd_set_two=self.sparksc.sparkContext.parallelize(range(1,20))
        rdd_op=rdd_set_one.union(rdd_set_two).distinct().collect()
        print(rdd_op)

    def name_to_county(self):
        row_names= self.get_first_column()
        namestoCounty=row_names.map(lambda n:(str(n[0]),str(n[1]))).groupByKey()
        print(namestoCounty.map(lambda x:{x[0]: list(x[1])}).collect())

    def sum_of_name(self):
        row_names= self.get_first_column()
        sum_of_names=row_names.filter(lambda line: "Count" not in line).map((lambda line: line.split(",")))
        get_count_name=sum_of_names.map(lambda n:(str(n[1],str(n[5])))).reduceByKey(lambda v1,v2:v1+v2).collect()

    def sortby_of_name(self):
        sum_of_names=self.getName.filter(lambda line: "Count" not in line).map((lambda line: line.split(",")))
        get_count_name=sum_of_names.map(lambda n:(str(n[1]),int(n[4]))).reduceByKey(lambda v1,v2:v1+v2).sortByKey().collect()
        print(get_count_name)
        self.spark_stop()

if __name__ == "__main__":
    mod_babynames = Mod_BabyNames()
    # mod_babynames.get_first_column()
    # mod_babynames.filter_names()
    # mod_babynames.rdd_parallel_union()
    # mod_babynames.rdd_parallel_intersect()
    # mod_babynames.rdd_parallel_distinct()
    # mod_babynames.name_to_county()
    mod_babynames.sortby_of_name()
