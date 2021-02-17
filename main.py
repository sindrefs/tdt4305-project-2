import argparse
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrameReader, SparkSession


def parse_path():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", help="Input path of data")
    args = parser.parse_args()

    if args.input_path is not None:
        return args.input_path
    else:
        return "data/"

def print_number_of_rows(rdd, rdd_name):
    print("There is {} rows in {}".format(rdd.count(), rdd_name))


def print_avg_post_length(rdd):
    post_length = rdd.map(lambda line: len(line))
    avg_length = post_length.reduce(lambda a, b: a+b)
    print(avg_length/rdd.count())


def main():
    path_to_data = parse_path()  # Parsing application specific arguments

    conf = SparkConf().setAppName("TDT4305 project").setMaster("local")
    sc = SparkContext(conf=conf)

    # Task 1
    badges_rdd = sc.textFile("{}/badges.csv".format(path_to_data))
    h = badges_rdd.first()
    badges_rdd = badges_rdd.filter(lambda x: x != h)

    comments_rdd = sc.textFile("{}/comments.csv".format(path_to_data))
    h = badges_rdd.first()
    comments_rdd = comments_rdd.filter(lambda x: x != h)

    posts_rdd = sc.textFile("{}/posts.csv".format(path_to_data))
    h = posts_rdd.first()
    posts_rdd = posts_rdd.filter(lambda x: x != h)

    users_rdd = sc.textFile("{}/users.csv".format(path_to_data))
    h = users_rdd.first()
    users_rdd = users_rdd.filter(lambda x: x != h)


    # Task 1.5
    print_number_of_rows(badges_rdd, "badges")
    print_number_of_rows(comments_rdd, "comments")
    print_number_of_rows(posts_rdd, "posts")
    print_number_of_rows(users_rdd, "users")


    #Task 2.1
    #print_avg_post_length(posts_rdd)



if __name__ == "__main__":
    main()