import argparse
import base64
from pyspark import SparkContext, SparkConf
from datetime import datetime
from operator import add
from math import sqrt, log2

from graphframes import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


# NB! Using pyspark 3.0.1

# TODO: Write that parsing from csv to "pretty" rdd could happen once

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


def task_1_5(badges_rdd, comments_rdd, posts_rdd, users_rdd):
    print_number_of_rows(badges_rdd, "badges")
    print_number_of_rows(comments_rdd, "comments")
    print_number_of_rows(posts_rdd, "posts")
    print_number_of_rows(users_rdd, "users")


def task_2_1(comments_rdd, posts_rdd):
    comment_column_length = comments_rdd.map(
        lambda line: len(base64.b64decode(line.split("\t")[2])))  # Comments text is located in column 2
    print("The average length of comment text is {}".format(comment_column_length.mean()))

    question_column_length = posts_rdd.map(
        lambda line: len(base64.b64decode(line.split("\t")[5]))
        if line.split("\t")[1] == '1' else -1)  # Answer body is in column 2 and PostTypeId in column 5 (1->question)
    question_column_length = question_column_length.filter(lambda x: x != -1)
    print("The average length of question body is {}".format(question_column_length.mean()))

    answer_column_length = posts_rdd.map(
        lambda line: len(base64.b64decode(line.split("\t")[5]))
        if line.split("\t")[1] == '2' else -1)  # Answer body is in column 2 and PostTypeId in column 5 (2->answer)
    answer_column_length = answer_column_length.filter(lambda x: x != -1)
    print("The average length of answer body is {}".format(answer_column_length.mean()))


def task_2_2(posts_rdd, users_rdd):
    questions = posts_rdd.map(lambda line: line.split("\t")).filter(
        lambda line: line[1] == '1')  # Filter out non-questions
    date_tuples = questions.map(
        lambda line: (line[6], datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')))  # Dates is in column 2
    latest_date_tuple = date_tuples.max(key=lambda x: x[1])
    oldest_date_tuple = date_tuples.min(key=lambda x: x[1])
    users_parsed = users_rdd.map(lambda line: line.split("\t"))
    user_of_latest_date_tuple = users_parsed.filter(lambda user: int(user[0]) == int(latest_date_tuple[0]))
    print("Username of latest post is {}".format(user_of_latest_date_tuple.first()[3]))
    user_of_oldest_date_tuple = users_parsed.filter(lambda user: int(user[0]) == int(oldest_date_tuple[0]))
    print("Username of oldest post is {}".format(user_of_oldest_date_tuple.first()[3]))


def task_2_3(posts_rdd):
    questions = posts_rdd.map(lambda line: line.split("\t")).filter(
        lambda line: line[1] == '1' and line[6] != '-1' and line[6] != 'NULL')  # Filter out non-questions
    questions_usernames = questions.map(lambda x: (x[6], 1))
    questions_username_max = questions_usernames.reduceByKey(add).max(key=lambda x: x[1])
    answers = posts_rdd.map(lambda line: line.split("\t")).filter(
        lambda line: line[1] == '2' and line[1] != '-1' and line[6] != 'NULL')  # Filter out non-answers
    answers_usernames = answers.map(lambda x: (x[6], 1))
    answers_username_max = answers_usernames.reduceByKey(add).max(key=lambda x: x[1])

    # TODO: Join with users to get DISPLAY NAME!
    print("The username that has posted the most questions is {} with {} posts".format(questions_username_max[0],
                                                                                       questions_username_max[1]))
    print("The username that has posted the most answers is {} with {} posts".format(answers_username_max[0],
                                                                                     answers_username_max[1]))


def task_2_4(badges_rdd):
    badges = badges_rdd.map(lambda line: line.split("\t"))  # Filter out non-questions
    badges_usernames = badges.map(lambda x: (x[0], 1))
    questions_username_count = badges_usernames.reduceByKey(add)
    questions_username_count_filtered = questions_username_count \
        .filter(lambda x: x[1] < 3)  # Filtering out users with strictly less than three badges
    print("There are {} users with strictly less than three badges".format(questions_username_count_filtered.count()))


def task_2_5(users_rdd):
    users_parsed = users_rdd.map(lambda line: line.split("\t"))
    users_with_votes = users_parsed \
        .map(lambda line: (line[0], int(line[7]), int(line[8])))  # User(0), upvote(1), downvote(2)
    only_upvotes = users_parsed.map(lambda line: int(line[7]))
    only_downvotes = users_parsed.map(lambda line: int(line[8]))
    avg_upvotes = only_upvotes.mean()
    avg_downvotes = only_downvotes.mean()
    diffs = users_with_votes.map(lambda user: (user[1] - avg_upvotes) * (user[2] - avg_downvotes))
    dividend = diffs.sum()
    diff_squared_upvotes = only_upvotes.map(lambda vote: (vote - avg_upvotes) ** 2)
    diff_squared_downvotes = only_downvotes.map(lambda vote: (vote - avg_downvotes) ** 2)
    divisor = sqrt(diff_squared_upvotes.sum()) * sqrt(diff_squared_downvotes.sum())
    r = dividend / divisor
    print("r_{XY} is {}".format(r))


def task_2_6(comments_rdd):
    comments = comments_rdd.map(lambda line: (line.split("\t")[4], 1))
    number_of_rows = comments.count()
    comments_reduced = comments.reduceByKey(add)
    terms = comments_reduced.map(lambda user: (user[1] / number_of_rows) * log2(user[1] / number_of_rows))
    h = -terms.sum()
    print("H(x) is {}".format(h))


def main():
    path_to_data = parse_path()  # Parsing application specific arguments

    conf = SparkConf().setAppName("TDT4305 project").setMaster("local")
    sc = SparkContext(conf=conf)


    #  Dependency and import for part 3
    sc.addPyFile("graphframes-0.8.1-spark3.0-s_2.12.jar")
    import graphframes

    # Task 1.1
    posts_rdd = sc.textFile("{}/posts.csv".format(path_to_data))
    h_posts = posts_rdd.first()
    posts_rdd = posts_rdd.filter(lambda x: x != h_posts)

    # Task 1.2
    comments_rdd = sc.textFile("{}/comments.csv".format(path_to_data))
    h_comments = comments_rdd.first()
    comments_rdd = comments_rdd.filter(lambda x: x != h_comments)

    # Task 1.3
    users_rdd = sc.textFile("{}/users.csv".format(path_to_data))
    h_users = users_rdd.first()
    users_rdd = users_rdd.filter(lambda x: x != h_users)

    # Task 1.4
    badges_rdd = sc.textFile("{}/badges.csv".format(path_to_data))
    h_badges = badges_rdd.first()
    badges_rdd = badges_rdd.filter(lambda x: x != h_badges)

    # Task 1.5
    print("\nTask 1.5 output below:")
    # task_1_5(badges_rdd, comments_rdd, posts_rdd, users_rdd)

    # Task 2.1
    print("\nTask 2.1 output below:")
    # task_2_1(comments_rdd, posts_rdd)

    # Task 2.2
    print("\nTask 2.2 output below:")
    # task_2_2(posts_rdd, users_rdd)

    # Task 2.3
    print("\nTask 2.3 output below:")
    # task_2_3(posts_rdd)

    # Task 2.4
    print("\nTask 2.4 output below:")
    # task_2_4(badges_rdd)

    # Task 2.5
    print("\nTask 2.5 output below:")
    # task_2_5(users_rdd)

    # Task 2.6
    print("\nTask 2.6 output below:")
    # task_2_6(comments_rdd)

    # Task 3.1
    # Creating the RDD in which later can be converted to DFs, then used for a graph
    sqlContext = SQLContext(sc)

    users_with_display_name = users_rdd \
        .map(lambda line: (line.split("\t")[0], line.split("\t")[3]))  # K->user id, V->DisplayName
    posts_with_user_id = posts_rdd \
        .map(lambda line: (line.split("\t")[0], line.split("\t")[6]))  # K->post id, V->OwnerUserId (user id)
    comments_with_user_id = comments_rdd \
        .map(lambda line: (line.split("\t")[0], line.split("\t")[4]))  # K->post id (from posts), V->UserID(user id)

    posts_and_comments_joined = posts_with_user_id \
        .join(comments_with_user_id)  # Performs join on post id, V->(user id of comment, user id of post)

    all_edges = posts_and_comments_joined \
        .map(lambda line: (line[1], 1))  # K->(user id of comment, user id of post), V->1
    edges_with_count = all_edges.reduceByKey(add)  # K->(user id of comment, user id of post), V->number of edges
    edges_with_count = edges_with_count\
        .map(lambda line: (line[0][0], line[0][1], line[1])) #  (user id of comment, user id of post, number of edges)

    # Task 3.2
    # Converting the RDDs to DFs of edges and vertices, then used to create a graph

    # Converting RDDs to DFs
    vertices = sqlContext.createDataFrame(users_with_display_name).toDF("id", "displayName")
    edges = sqlContext.createDataFrame(edges_with_count).toDF("src", "dst", "w")

    edges.registerTempTable("edges")

    res = sqlContext.sql("SELECT src, SUM(w) FROM edges GROUP BY src ORDER BY SUM(w) DESC")
    res.show()

    '''
    # Create the graph using GraphFrame
    graph = graphframes.GraphFrame(vertices, edges)

    # Query: Get in-degree of each vertex.
    #graph.inDegrees.show()
    top = graph.pageRank(resetProbability=0.1, maxIter=1)
    top.outDegrees.show()
    '''
    '''
    # Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    # Run PageRank algorithm, and show results.
    results = g.pageRank(resetProbability=0.01, maxIter=20)
    results.vertices.select("id", "pagerank").show()
    '''


if __name__ == "__main__":
    main()
