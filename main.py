import argparse
from pyspark import SparkContext, SparkConf
import base64
from datetime import datetime

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

'''
def find_avg_length(rdd, column_number, decode=False, req=None):  # TODO: Mark decode to True as default

    if req is not None:
        column_length = rdd.map(lambda line:
                                len(base64.b64decode(line).split("\t")[column_number])
                                if decode else
                                len(line.split("\t")[column_number]))
    else:
        column_length = rdd.map(lambda line:
                                len(base64.b64decode(line).split("\t")[column_number])
                                if decode else
                                len(line.split("\t")[column_number]))

    total_length = column_length.reduce(lambda a, b: a + b)
    avg_length = total_length / rdd.count()
    return avg_length
'''

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


def main():
    path_to_data = parse_path()  # Parsing application specific arguments

    conf = SparkConf().setAppName("TDT4305 project").setMaster("local")
    sc = SparkContext(conf=conf)

    # Task 1
    badges_rdd = sc.textFile("{}/badges.csv".format(path_to_data))
    h_badges = badges_rdd.first()
    badges_rdd = badges_rdd.filter(lambda x: x != h_badges)

    comments_rdd = sc.textFile("{}/comments.csv".format(path_to_data))
    h_comments = comments_rdd.first()
    comments_rdd = comments_rdd.filter(lambda x: x != h_comments)

    posts_rdd = sc.textFile("{}/posts.csv".format(path_to_data))
    h_posts = posts_rdd.first()
    posts_rdd = posts_rdd.filter(lambda x: x != h_posts)

    users_rdd = sc.textFile("{}/users.csv".format(path_to_data))
    h_users = users_rdd.first()
    users_rdd = users_rdd.filter(lambda x: x != h_users)

    # Task 1.5
    print("\nTask 1.5 output below:")
    #task_1_5(badges_rdd, comments_rdd, posts_rdd, users_rdd)

    # Task 2.1
    print("\nTask 2.1 output below:")
    #task_2_1(comments_rdd, posts_rdd)

    # Task 2.2
    print("\nTask 2.2 output below:")
    #task_2_2(posts_rdd, users_rdd)

    # Task 2.3
    questions = posts_rdd.map(lambda line: line.split("\t")).filter(
        lambda line: line[1] == '1' and line[6] != '-1')  # Filter out non-questions
    questions_usernames = questions.map(lambda x: x[0])
    answers = posts_rdd.map(lambda line: line.split("\t")).filter(
        lambda line: line[1] == '2' and line[1] != '-1')  # Filter out non-answers
    answers_usernames = answers.map(lambda x: x[0])





if __name__ == "__main__":
    main()
