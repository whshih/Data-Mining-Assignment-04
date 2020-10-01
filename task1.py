from sys import argv
import itertools
from graphframes import *
from pyspark.sql import SQLContext
import ellyLib


# Check if there is an edge between nodes.
def edge_check(user_bus, user_dic, threshold):
    edge = []
    for i in user_dic:
        if user_bus[0] != i:
            if len(user_bus[1].intersection(user_dic[i])) >= threshold:
                edge.append((user_bus[0], i))
    return [e for e in edge if e is not None]


def n(diff_node, user_dic):
    return diff_node, list(user_dic[diff_node])


def main():
    threshold = 7
    input_file = 'ub_sample_data.csv'
    output_file = 'task1.txt'

    sc = ellyLib.start_spark('Task1', 'local[3]')
    # user with business_set (load the data after row_num 2)
    rddtext = sc.textFile(input_file).mapPartitionsWithIndex(
        lambda x, f: itertools.islice(f, 1, None) if x == 0 else f).map(
        lambda line: line.split(',')).groupByKey().map(lambda x: (x[0], set(x[1])))
    # Build dictionary for rddtext
    user_dic = rddtext.collectAsMap()

    # Check if there is an edge between nodes. edge_num = 996
    edges = rddtext.flatMap(lambda x: edge_check(x, user_dic, threshold))
    # We count the nodes with edge and the counterpart of nodes are unnecessary. nodes_num = 222
    nodes = edges.map(lambda x: x[0]).distinct().map(lambda x: n(x, user_dic)).map(lambda line: (line[0],))

    # CreateDataFrame
    v = SQLContext(sc).createDataFrame(nodes, ["id"])
    e = SQLContext(sc).createDataFrame(edges, ["src", "dst"])

    graph = GraphFrame(v, e)

    # labelPropagation_output: id:str, business:list, label:num
    LPA = graph.labelPropagation(maxIter=5)

    # label(1): id_list
    # We are required to print our output with the order firstly sorting by the size of communities, from min to max, then sorting by the alphabet a-z.
    # Print the communities consisting of id.
    group_community = LPA.rdd.map(lambda x: (x["label"], [x["id"]])).reduceByKey(
        lambda x, y: x + y).map(lambda x: (len(x[1]), sorted(x[1])))
    result = group_community.sortBy(lambda x: (x[0], x[1])).map(lambda x: x[1]).collect()

    # Output_format
    with open(output_file, 'w') as f:
        for i in result:
            if len(i) == 1:
                f.write('\'' + str(i).replace("['", "").replace("']", "") + '\'')
                f.write('\n')
            else:
                f.write(', '.join(['\'' + j + '\'' for j in i]))
                f.write('\n')


if __name__ == "__main__":
    main()