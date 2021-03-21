import argparse
import base64
from datetime import datetime

# Not in use for this part of the project
from graphframes import *
# from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import *


def parse_path():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", help="Input path of data")
    parser.add_argument("-p", "--post_id", help="Post id of post to examine")
    args = parser.parse_args()

    path = None
    if args.input_path is not None:
        path = args.input_path
    else:
        path = "data/"  # Default location if flag is not specified

    return path, str(args.post_id)


# Takes a list of strings, and removed (from within) the bad strings from the original string
def remove_strs_from_string(original_string, bad_strings):
    new_string = str(original_string)
    for bad_str in bad_strings:
        new_string = new_string.replace(bad_str, "")
    return new_string


# Takes a list of strings, and strips (from each end) the bad strings from the original string
def strip_strs_from_string(original_string, bad_strings):
    new_string = str(original_string)
    for bad_str in bad_strings:
        new_string = new_string.strip(bad_str)
    return new_string


# Read stopwords from a local file
def read_stopwords(path):
    list_of_stopwords = []

    try:
        f = open(path, 'r')

        for line in f.readlines():
            list_of_stopwords.append(line.strip())
        print("Stopwords successfully read from local file.")
    except:
        print("Something went wrong while reading stopwords from local file.")

    return list_of_stopwords


# Takes a list of terms an a windows size. Returns a list of lists of each windows (after sliding over the term list)
def generate_sliding_windows(terms, window_size):
    n_windows = (len(terms) - window_size) + 1
    windows = []
    for i in range(n_windows):
        windows.append(terms[i:(i + window_size)])
    return windows


# Takes a list of windows. Returns a list of tuples representing edges between each term within the same window
def generate_graph_tuples(windows):
    graph = []
    for window in windows:
        for i in range(len(window)):
            for j in range(i + 1, len(window)):
                if i != j:
                    graph.append((window[i], window[j]))  # Edge from i to j
                    graph.append((window[j], window[i]))  # Edge from j to i

    return graph


def main():
    import findspark
    # Local path to where spark folder is located
    # Note that "graphframes(..).jar" is placed in the spark folder
    # Remove line below to use pure pyspark, however "graphframes(..).jar" must then be placed in the env directory
    findspark.init("/Users/sindresorensen/spark-3.0.2-bin-hadoop3.2")

    path_to_data, post_id = parse_path()  # Parsing application specific arguments

    spark = SparkSession.builder.master("local[*]").appName("TDT4305 project 2").getOrCreate()
    sc = spark.sparkContext

    posts_rdd = sc.textFile("{}/posts.csv".format(path_to_data))
    h_posts = posts_rdd.first()
    posts_rdd = posts_rdd.filter(lambda x: x != h_posts)  # Remove header

    # Filter out only the correct post
    posts_rdd = posts_rdd.map(lambda line: line.split("\t")).filter(lambda line: line[0] == post_id)

    # Take only body column, decode base64 and convert to lowercase
    body_rdd = posts_rdd.map(lambda line: base64.b64decode(line[5]).lower())

    bad_strs = ["!", "?", "#", "$", "%", "&", "+", "-", "<p>", "<", ">", "@", "\t", "\n", "\r", "\\", "/"]
    body_filtered_rdd = body_rdd.map(lambda line: remove_strs_from_string(line, bad_strs))  # Remove bad strings

    body_filtered_rdd = body_filtered_rdd.map(lambda line: line.split(" "))  # Split/tokenize at whitespace

    # Take first entry in body_filtered_rdd, and converts its list to a new rdd for further processing
    terms_rdd = sc.parallelize(body_filtered_rdd.first())

    # Only keeps strings strictly longer than 2 chars
    terms_rdd = terms_rdd.filter(lambda term: len(term) > 2)

    bad_ends = [".", ",", ";", ":"]
    terms_rdd = terms_rdd.map(lambda term: strip_strs_from_string(term, bad_ends))  # Strip away bad strings

    list_of_stopwords = read_stopwords("big_list_of_english_stopwords.txt")  # Read stopwords from local txt file
    terms_rdd.filter(lambda term: term not in list_of_stopwords)  # Remove stopwords

    # print(terms_rdd.collect())

    terms_unique_rdd = terms_rdd.distinct()  # Remove duplicates
    terms_unique_rdd = terms_unique_rdd.map(lambda term: (term,))  # Create tuples of terms (for compatibility later)

    windows = generate_sliding_windows(terms_rdd.collect(), 5)  # Generate list of windows
    graph = generate_graph_tuples(windows)  # Generate edge tuples
    graph_rdd = sc.parallelize(graph)  # Create rdd from graph (ie. edge tuples)
    # print(graph_rdd.collect())

    v = spark.createDataFrame(terms_unique_rdd, ['id'])  # Create DF of vertices (distinct terms)
    e = spark.createDataFrame(graph_rdd, ['src', 'dst'])  # Create DF of edges (edge tuples rdd)

    g = GraphFrame(v, e)  # Create graphframes graph of vertices and edges
    #g.vertices.show()  # Top vertices of g
    #g.edges.show()  # Top edges of g
    # Check the number of edges of each vertex
    #g.degrees.show()  # Top degrees of g

    print("Pagerank started at {}".format(datetime.now().time()))
    pr = g.pageRank(resetProbability=0.15, tol=0.0001)
    print("Pagerank completed at {}".format(datetime.now().time()))
    print("Top 20 vertices with pagerank value below")
    pr.vertices.sort('pagerank', ascending=False).show()


if __name__ == "__main__":
    main()
