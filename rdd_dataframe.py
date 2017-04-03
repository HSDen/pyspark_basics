__author__ = 'Hari, Gnanaprakasam'

from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os.path
import atexit


####################################################################################################
# Basic Spark RDD
# Class: rdd_dataframe
# Method(s):
#
####################################################################################################




class RDD_Dataframe():

    FILE_NAME = "Baby_Names__Beginning_2007.csv"
    PAR_FILE="region.parquet"
    TXT_FILE="Baby_Names__Beginning_2007.txt"
    file_relative_path = os.path.dirname(__file__) + "/" + FILE_NAME
    par_file=os.path.dirname(__file__) + "/" + PAR_FILE
    txt_file=os.path.dirname(__file__) + "/" + TXT_FILE
    sparksc = SparkSession.builder.master("local").appName("RDD_SQL").getOrCreate()
    getName = sparksc.sparkContext.textFile(file_relative_path)


    def spark_stop(self):
        print("*******Closing All Spark sessions****************")
        atexit.register(self.sparksc.stop)

    def rdd_csv_read(self):
        rdd_csv_filename= self.sparksc.read.\
                      format('com.databricks.spark.csv').\
                      options(header='true',inferschema='true').load(self.file_relative_path)

        rdd_csv_filename.registerTempTable('BabyNames')
        get_county_names=self.sparksc.sql("""select county from BabyNames where FirstName=`Gavin` """)

        for countnames in get_county_names.collect():
            print(countnames)

    def read_parqt_data(self):
        df_par_path=self.sparksc.read.parquet(self.par_file)
        df_par_path.printSchema()
        df_par_path.select("R_REGIONKEY").show()
        df_par_path.filter(df_par_path['R_REGIONKEY']>1).show()

    def rdd_grp_oprations(self):
        df_par_path=self.sparksc.read.parquet(self.par_file)
        df_par_path.groupBy("R_NAME").count().show()
    def rdd_temp_view(self):
        df_par_path=self.sparksc.read.parquet(self.par_file)
        df_par_path.createOrReplaceTempView("region")
        self.sparksc.sql("select * from region").show()

    def rdd_dataframe_create(self):
        rws= self.sparksc.sparkContext.textFile(self.txt_file)
        df_region_table= rws.map(lambda l: l.split(","))
        for b in df_region_table.collect():
            print (b)



if __name__ == "__main__":
    rdd_df = RDD_Dataframe()
    rdd_df.rdd_csv_read()
    rdd_df.read_parqt_data()
    rdd_df.rdd_grp_oprations()
    rdd_df.rdd_temp_view()
    rdd_df.rdd_dataframe_create()