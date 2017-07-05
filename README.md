# anomaly_detection

# Compiling the Program
I am using C++11. So it will not compile with older C++ compiler. If the program does not compile please check the compile string in run.sh. Once it compiles, the program should work.


# Graph Representation
The users can add and remove friends any time. So I represnted the adjacency list of a user by hash table (unordered_map). This takes expected constant time to add or delete a friend. Also, the user id could be a huge number even though there may not be many users. So I use a hash table to map a user id to its adjacency list (which is again a hashtable). Thus the graph is represneted using a hash table whose keys are the user ids and the values are hash tables.

We need to keep track of the purchases each user is making. To represnt this I use a hashtable with keys as user ids and the values as deques (double ended queue).  I use a deque because we determine if a purchase amount is anomalous or not by using only the last T transactions. Thus if a user makes more than T purchases we can remove the old purchases. This insertion and deletion in deque is constant time.  

Now when a user u makes a purchase we can do a bfs (breadth first search) on the graph and find the neighbors that are atmost D distance away from u. We add all the purchases made by these neighbors to a vector and then sort the vector in decreasing order of timestamp. We consider only the first T purchases to compute mean and standard deviation to determine if the purchase by user u is anomalous or not.  If the purchase is anomalous it is written to the output file.   

