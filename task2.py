import sys
import itertools
from queue import Queue
import elly_func


class tree:

    def __init__(self, user_id):
        self.user_id = user_id
        self.Lv = 0
        self.parent = set()
        self.v0 = 0
        self.vn = 1.0
        self.community = -1

    def setup_Lv(self, newLv):
        self.Lv = newLv

    def parent_revise(self, node):
        self.parent.add(node)

    def v0_revise(self, x):
        self.v0 += x

    def vn_revise(self, x):
        self.vn += x


# Check if there is an edge between nodes.
def edge_check(user_bus, user_dic, threshold):
    edge = []
    for i in user_dic:
        if user_bus[0] != i:
            if len(user_bus[1].intersection(user_dic[i])) >= threshold:
                edge.append((user_bus[0], [i]))
    return [e for e in edge if e != None]


def n(diff_node, user_dic):
    return diff_node, list(user_dic[diff_node])


def Girvan_Newman(node0: str, node_chain: dict):
    node_dic = {}
    node = tree(node0)
    node.v0 = 1
    node_dic[node0] = node
    next_candidate = Queue()
    next_candidate.put(node)

    seen = set()
    seen.add(node0)

    # Receive distance between two nodes.
    while not next_candidate.empty():
        node1 = next_candidate.get()
        for n in node_chain[node1.user_id]:
            node_downstair = tree(n)
            if n not in seen:
                seen.add(node_downstair.user_id)
                next_candidate.put(node_downstair)
                node_downstair.setup_Lv(node1.Lv + 1)
                node_downstair.v0_revise(node1.v0)
                node_downstair.parent_revise(node1.user_id)
                node_dic[node_downstair.user_id] = node_downstair
            else:
                node_downstair = node_dic[n]
                if node_downstair.Lv > node1.Lv:
                    node_downstair.v0_revise(node1.v0)
                    node_downstair.parent_revise(node1.user_id)

    L = []
    for i in node_dic.values():
        tup = (i.Lv, i.user_id)
        L.append(tup)
    Level = {}
    for i in range(len(L)):
        if L[i][0] in Level.keys():
            Level[L[i][0]].add(L[i][1])
        else:
            Level[L[i][0]] = {L[i][1]}

    def frac(front_v2):
        front_index = []
        for i in front_v2:
            sum_up = sum(front_v2)
            new = i / sum_up
            front_index.append(new)
        return front_index

    btw = []

    for i in range(max(Level.keys()), 0, -1):
        horizon = Level[i]

        for chain in horizon:
            parent_v0 = []
            node = node_dic[chain]
            parent_node = list(node.parent)
            #             print(parent_node)
            for front in parent_node:
                parent_v0.append(node_dic[front].v0)

            parent_w = frac(parent_v0)
            #             print(parent_w)
            for i in range(len(parent_w)):
                user_id = parent_node[i]
                vn_1 = float(node.vn * parent_w[i])
                node_dic[user_id].vn_revise(vn_1)
                btw.append((tuple(sorted((node.user_id, user_id))), vn_1 / 2))
    return btw


def modularity(C: list, q_dict: dict):
    q = 0
    for c in C:
        if c[0] > 1:
            for i in c[1]:
                for j in c[1]:
                    q = q_dict[(i, j)] + q
    return q


def make_tuple(x):
    temp = []
    for i in range(len(x[1])):
        temp.append((x[0], x[1][i]))
    return temp


def A(x):
    return ((x[0], x[1]), 1)


def make_Degree(x):
    return ((x[0], x[0]), len(x[1]))


def q_booklet(A: dict, n: list, btw_num: int, Degree: dict):
    q = {}
    m = btw_num
    for i in range(len(n)):
        for j in range(len(n)):
            if (n[i], n[j]) in A.keys():
                q[(n[i], n[j])] = (1 - (Degree[n[i], n[i]] * Degree[n[j], n[j]]) / float(2 * m)) / float(2 * m)
            else:
                q[(n[i], n[j])] = (0 - (Degree[n[i], n[i]] * Degree[n[j], n[j]]) / float(2 * m)) / float(2 * m)
    return q


def construct_community(n: list, node_chain: dict):
    c = []
    for node in n:
        if node_chain[node] == []:
            c.append((1, node))
            node_chain.pop(node)

    while bool(node_chain):
        node = list(node_chain.keys())
        local_n = [node[0]]
        seen = {node[0]}
        j = 0
        while j < len(local_n):
            for i in node_chain[local_n[j]]:
                if i not in seen:
                    seen.add(i)
                    local_n.append(i)
            node_chain.pop(local_n[j])
            j += 1
            num_n_per_c = len(local_n)
        c.append((num_n_per_c, sorted(local_n)))
    return c


def main():
    threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file1 = sys.argv[3]
    output_file2 = sys.argv[4]

    # user with business_set (load the data after row_num 2)
    sc = ellyLib.start_spark('Assignment4 Task2')
    rddtext = sc.textFile(input_file).mapPartitionsWithIndex(
        lambda x, f: itertools.islice(f, 1, None) if x == 0 else f).map(
        lambda line: line.split(',')).groupByKey().map(lambda x: (x[0], set(x[1])))
    # Build dictionary for rddtext
    user_dic = rddtext.collectAsMap()

    # Check if there is an edge between nodes. node_nodeList = 222
    edges = rddtext.flatMap(lambda x: edge_check(x, user_dic, threshold)).reduceByKey(lambda a, b: a + b).sortByKey()
    node_node_dic = edges.collectAsMap()
    # We count the nodes with edge and the counterpart of nodes are unnecessary. # nodes_num = 222 [node1, node2,....]
    nodes = edges.map(lambda x: x[0])
    n = nodes.collect()

    betweenness = edges.flatMap(lambda x: Girvan_Newman(x[0], node_node_dic)).reduceByKey(
        lambda a, b: a + b).sortBy(lambda b: (-b[1], b[0]))

    btw = betweenness.collect()
    new_betweenness = betweenness
    btw_num = len(btw)

    with open(output_file1, 'w') as f:
        for i in range(btw_num):
            result = '(' + btw[i][0][0] + ', ' + btw[i][0][1] + '), ' + str(btw[i][1])
            f.write(result)
            f.write('\n')

    # Build Aij and Degree matrix
    Aij = edges.map(make_tuple).flatMap(lambda x: x).map(A).collectAsMap()
    Degree = edges.map(make_Degree).collectAsMap()

    q1 = 1
    q0 = 0
    # Build a dictionary to checkup the calculation for modularity
    q_dic = q_booklet(Aij, n, btw_num, Degree)

    # Our main goal is to find the Q, the large the better!!
    while q1 > q0:
        btwr = new_betweenness.map(lambda x: (x[0][0], x[0][1])).first()

        # Remove the edge and update the relationship btw nodes
        node_node_dic[btwr[0]].remove(btwr[1])
        node_node_dic[btwr[1]].remove(btwr[0])

        # Construct the community
        C = sorted(construct_community(n, node_node_dic.copy()), key=lambda x: (x[0], x[1]))

        # After cuting an edge we need to compute and update btwnness for every edges
        e = sc.parallelize(node_node_dic.items())
        new_betweenness = e.flatMap(lambda x: Girvan_Newman(x[0], node_node_dic)).reduceByKey(
            lambda a, b: a + b).sortBy(lambda x: (x[1], x[0][0], x[0][1]))

        # Update the original value for q0 then back to loop if q0 still less than q1
        q0 = q1
        q1 = modularity(C, q_dic)

        # Finally, we obtain nice communities by getting the highest q
        C_final = C

    with open(output_file2, 'w') as f:
        # tuple(num,(nodeA,nodeB))
        for num, pair in C_final:
            result = str(pair)
            f.write(result[1:-1])
            f.write('\n')


if __name__ == '__main__':
    main()