import json

def start_spark(name = 'SparkTask', local = 'local[*]'):
    from pyspark import SparkContext
    from pyspark import SparkConf

    confSpark = SparkConf().setAppName(name).setMaster(
        local).set('spark.driver.memory', '4G').set(
            'spark.executor.memory', '4G')
    sc = SparkContext.getOrCreate(confSpark)
    sc.setLogLevel(logLevel='ERROR')
    return sc

def tojson(line):
    lines = line.splitlines()
    data = json.loads(lines[0])
    return data

