import argparse
import base64
from pyspark import *  # Assuming pyspark 3.0.1
from pyspark.sql import *
from datetime import datetime
from operator import add
from math import sqrt, log2

# Not in use for this part of the project
from graphframes import *
# from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import *


def parse_path():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", help="Input path of data")
    args = parser.parse_args()

    if args.input_path is not None:
        return args.input_path
    else:
        return "data/"  # Default location if flag is not specified


def main():
    path_to_data = parse_path()  # Parsing application specific arguments

    spark = SparkSession.builder.appName('fun').getOrCreate()
    vertices = spark.createDataFrame([('1', 'Carter', 'Derrick', 50),
                                      ('2', 'May', 'Derrick', 26),
                                      ('3', 'Mills', 'Jeff', 80),
                                      ('4', 'Hood', 'Robert', 65),
                                      ('5', 'Banks', 'Mike', 93),
                                      ('98', 'Berg', 'Tim', 28),
                                      ('99', 'Page', 'Allan', 16)],
                                     ['id', 'name', 'firstname', 'age'])
    edges = spark.createDataFrame([('1', '2', 'friend'),
                                   ('2', '1', 'friend'),
                                   ('3', '1', 'friend'),
                                   ('1', '3', 'friend'),
                                   ('2', '3', 'follows'),
                                   ('3', '4', 'friend'),
                                   ('4', '3', 'friend'),
                                   ('5', '3', 'friend'),
                                   ('3', '5', 'friend'),
                                   ('4', '5', 'follows'),
                                   ('98', '99', 'friend'),
                                   ('99', '98', 'friend')],
                                  ['src', 'dst', 'type'])
    g = GraphFrame(vertices, edges)
    ## Take a look at the DataFrames
    g.vertices.show()
    g.edges.show()
    ## Check the number of edges of each vertex
    g.degrees.show()


if __name__ == "__main__":
    main()
