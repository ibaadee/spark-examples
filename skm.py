
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
# $example on$
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
conf = SparkConf().setAppName('KMeansExample')
sc = SparkContext(conf=conf)


# In[6]:


data = sc.textFile("data.csv")
parsedData = data.map(lambda line: array([x for x in line.split(',')]))


# In[7]:


print(parsedData.take(5))


# In[16]:


# take parameters only
paramsOnly = parsedData.map(lambda x: array([float(x[0]),float(x[1]),float(x[2]),float(x[3])]))


# In[17]:


paramsOnly.take(5)


# In[18]:


# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


# In[19]:


WSSSE = paramsOnly.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))


# In[24]:


# Try with a range of number of clusters
for i in range (1, 6):
    clusters = KMeans.train(paramsOnly, i, maxIterations=100, initializationMode="random")
    WSSSE = (paramsOnly.map(lambda point: error(point)).reduce(lambda x, y: x + y))
    print("With " + str(i) + " clusters: Within Set Sum of Squared Error + " + str(WSSSE))


# In[3]:


data.show()

