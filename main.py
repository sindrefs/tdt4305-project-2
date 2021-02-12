import argparse
from pyspark import SparkContext, SparkConf


def parse_path():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", help="Input path of data")
    args = parser.parse_args()

    if args.input_path is not None:
        return args.input_path
    else:
        return "data/"


def main():
    path_to_data = parse_path()

    conf = SparkConf().setAppName("TDT4305 project").setMaster("local")
    sc = SparkContext(conf=conf)

    badges_rdd = sc.textFile("{}/badges.csv".format(path_to_data))
    comments_rdd = sc.textFile("{}/comments.csv".format(path_to_data))
    posts_rdd = sc.textFile("{}/posts.csv".format(path_to_data))
    users_rdd = sc.textFile("{}/users.csv".format(path_to_data))


    print("There is {} rows in {}".format(badges_rdd.count(), "badges"))
    print("There is {} rows in {}".format(comments_rdd.count(), "comments"))
    print("There is {} rows in {}".format(posts_rdd.count(), "posts"))
    print("There is {} rows in {}".format(users_rdd.count(), "users"))


if __name__ == "__main__":
    main()