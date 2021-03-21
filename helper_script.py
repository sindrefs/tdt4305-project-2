
l = []

try:
    f = open("big_list_of_english_stopwords.txt", 'r')

    for line in f.readlines():
        l.append(line.strip())
except:
    print("Something went wrong.")

print(l)

