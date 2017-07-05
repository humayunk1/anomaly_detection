#include <iostream>
#include <string>
#include <fstream>
#include <cstdlib>
#include <cmath>
#include <vector>
#include <deque>
#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cmath>

using namespace std;

typedef unsigned int vid_t;

/* A struct to represent purchage */
struct Purchase {
    string orgTimeStamp;
    string timeStamp;
    string strId;
    vid_t id;
    string strAmount;
    double amount;

    void display() {
	cout<<"purchase object contains: \n";
        cout<<"Id :"<< id << endl;
        cout<<"Amount :"<< amount << endl;
        cout<<"TimeStamp :"<< timeStamp << endl;
    }

    string returnWriteString() {
	string res =  "{\"event_type\":\"purchase\", \"timestamp\":" + orgTimeStamp;
	
	res +=  ", \"id\":" + strId + ", \"amount\":" + strAmount + "";
	return res; 
    }
};

/* compare two purchase object in decreasing order of time */
bool compPurchases(const Purchase& lhs, const Purchase& rhs) {
    return lhs.timeStamp > rhs.timeStamp;
}

/*Compute mean of first N elements in the vector*/ 
double mean(vector<Purchase>& purchases, size_t N);

/*Compute SD of first N elements in the vector*/ 
double standardDediation(vector<Purchase>& purchases, double mu, size_t N);

//Define the adj list and purchase lists of the users 
//the adjacencies of the vertices are represented as hashTable -- so friends can be added and deleted in
//expected constant time 
typedef unordered_map< vid_t, unordered_map<vid_t, bool> > adjMapType;    

//List of Purchases of a user is represented using a Deque -- this is because if the size becomes
//larger than T -- the oldest purchase can be deleted -- since only last T purchases are considered
//each user needs to keep track of last T purchases only 
typedef unordered_map< vid_t, deque< Purchase > > adjPurchaseType;
 
//each user needs to keep track of last T purchases only 
struct graph_t {
    unsigned int D;
    unsigned int T;
    ofstream outFile;
 

    //adjacencies
    adjMapType adj;    

    //puchases for each user
    adjPurchaseType userPurchases;    

    //id1 and id2 become friend
    void addFriend(vid_t id1, vid_t id2) {

	adjMapType::iterator it = adj.find(id1-1);
	//If the (id1-1) -- exists in the graph
	if( it != adj.end() ) {   
	    //insert id2-1 in (id1-1)'s adj list
	    it->second.insert( make_pair< vid_t, bool >( id2-1, true) );
	}
	else {
	    //If id1-1 does not exist -- create a friend list and add 
	    unordered_map<vid_t, bool>  adjId1;
	    adjId1.insert( make_pair<vid_t, bool>(id2-1, true) );
	    adj[id1-1] = adjId1; 
	}

	it = adj.find(id2-1);

	//If the (id2-1) -- exists in the graph
	if( it != adj.end() ) {   
	    //insert id1-1 in (id2-1)'s adj list
	    it->second.insert( make_pair< vid_t, bool >( id1-1, true) );
	}
	else {
	    //If id2-1 does not exist -- create a friend list and add 
	    unordered_map<vid_t, bool>  adjId2;
	    adjId2.insert( make_pair<vid_t, bool>(id1-1, true) );
	    adj[id2-1] = adjId2; 
	}
    }

    //id1 and id2 are ending their friendship 
    void deleteFriend(vid_t id1, vid_t id2) {

	adjMapType::iterator it = adj.find(id1-1);
	if( it != adj.end() ) {
	   //delete (id2-1) from (id1-1)'s adj list 
	   it->second.erase( id2-1 );
	}
	else {
	    cout<<"the user: " << id1 <<"  does not exist in netowrk: so friends can't be deleted \n";
	}


	it = adj.find(id2-1);
	if( it != adj.end() ) {
	   //delete (id1-1) from (id2-1)'s adj list 
	   it->second.erase( id1-1 );
	}
	else {
	    cout<<"the user: " << id2 <<"  does not exist in netowrk: so friends can't be deleted \n";
	}
    }

   //Find all the neighbors at most D distance away from s
    void bfs(vid_t s, vector < vid_t > & neighborsInDDist) {
	
	adjMapType::iterator it = adj.find(s);
	if( it == adj.end() ) {
	    cout<<"In bfs: User: " <<s <<" does not exist\n";
	    return;
	}

	//a hashTable to mark user visited or unvisited
	unordered_map<vid_t, bool> visited;

	//mark all the users as unvisited in the beginning
	for(it = adj.begin(); it != adj.end(); ++it) {
	    visited[it->first] = false;
	}

	vector < vid_t > vectorA;
	vector < vid_t > vectorB;

	vectorA.push_back( s );
	visited[s] = true;

	unsigned int d = 0;
	while( d < D ) {

	    if( vectorA.size() == 0 ) {
		break;
	    }

	    for(size_t i = 0; i < vectorA.size(); i++) {
		vid_t vertex = vectorA[i];

		//visit the neighbors of: vertex
		for( unordered_map<vid_t, bool>::iterator it = adj[vertex].begin(); it != adj[vertex].end(); ++it) {
		    vid_t neighbor = it->first;

		    if( visited[ neighbor ]  == false ) {
			neighborsInDDist.push_back( neighbor );
			vectorB.push_back( neighbor );
			visited[ neighbor ] = true;
		    }
		}
	    }

	    //swap vectorA and vectorB
	    vectorA.clear();

	    vector< vid_t > temp = vectorA;
	    vectorA = vectorB;
	    vectorB = temp;
	

            d++;
	}
    } 

    //add purchase for a user
    void addPurchase( Purchase purchaseObj ) {

	vid_t id = purchaseObj.id -1;
	adjPurchaseType::iterator it = userPurchases.find( id );

	//If the user exist
	if( it != userPurchases.end() ) {
	    it->second.push_back( purchaseObj );

	    //If the user has purchased more than T times -- delete the oldest purchase
	    if( it->second.size() > T ) {
		it->second.pop_front();
	    }
	}
	else {
	    deque<Purchase> purchaseDeque;
	    purchaseDeque.push_back( purchaseObj );
	    userPurchases.insert( make_pair<vid_t, deque< Purchase > > ( id, purchaseDeque ) );
	}

    }

    //add purchase and check if it is anomalous
    void addPurchaseandCheckAnomalies(Purchase purchaseObj) {

	vid_t id = purchaseObj.id -1;
	adjPurchaseType::iterator it = userPurchases.find( id );

	//If the user exist
	if( it != userPurchases.end() ) {
	    it->second.push_back( purchaseObj );

	    //If the user has purchased more than T times -- delete the oldest purchase
            if( it->second.size() > T ) {
                it->second.pop_front();
            }

	}
	else {
	    deque<Purchase> purchaseDeque;
	    purchaseDeque.push_back( purchaseObj );
	    userPurchases.insert( make_pair<vid_t, deque< Purchase > > ( id, purchaseDeque ) );
	}

	//Check if this purchase is anomalous
	computeAnomalies(id, purchaseObj);
	
    }

    //Given a purchase -- checks if it is anomalous or not
    //If the purchase is anomalous writes it to a file
    void computeAnomalies(vid_t id, Purchase purchaseObj) {
	//Find the users D distance away from id 
	//These are going to be affected by this purchase
	vector< vid_t > neighbors;
	
	this->bfs(id, neighbors); 
	//cout<<"After BFS "<<endl;
        
	//Make a vector of purchases of all the neighbors within distance D 
	vector<Purchase> neighborPurchases;
	for(size_t i = 0; i < neighbors.size(); i++) {
	    vid_t n = neighbors[i];

	    adjPurchaseType::iterator it = userPurchases.find( n );
	    if( it != userPurchases.end() ) { 
		for(deque<Purchase>::iterator purchaseIt = it->second.begin(); purchaseIt != it->second.end(); purchaseIt ++) {
		    neighborPurchases.push_back( *purchaseIt );
		}  
	    }
	}

        double amount = purchaseObj.amount;

	//If there are atleast 2 purchases --then check for anomaly
	if( neighborPurchases.size() > 1 ) {

	   if( neighborPurchases.size() < T ) {
		//sort the purchases in neighborPurchases -- using stbale sort -- in decreasing order of timestamp
		stable_sort(neighborPurchases.begin(), neighborPurchases.end(), compPurchases);

		double mu = mean(neighborPurchases, neighborPurchases.size() );		
		double sd = standardDediation( neighborPurchases, mu, neighborPurchases.size() );

	        //check if the amount is anomalous
		if( amount > mu + 3 * sd ) {

		    if( outFile.is_open() ) {
			string str = purchaseObj.returnWriteString(); 

			stringstream strStream;
			strStream.precision(2);
			strStream << fixed; 

			mu = trunc( mu * 100 ) / 100;
			strStream << mu;
			string muStr = strStream.str();
			strStream.str("");

			sd = trunc( sd * 100 ) / 100;
			strStream << sd;
			string strSd = strStream.str();
			str += ", \"mean\": \""+muStr+"\", \"sd\": \""+strSd+"\"}"; 

			//Write the anomalous purchase to the output file	
			outFile<< str << "\n";
		    }
		}

	   }
	   else {
		//sort the purchases in neighborPurchases -- using stbale sort -- in decreasing order of timestamp
		stable_sort(neighborPurchases.begin(), neighborPurchases.end(), compPurchases);

		double mu = mean(neighborPurchases, T);		
		double sd = standardDediation( neighborPurchases, mu, T);

		//check if the amount is anomalous
		if( amount > mu + 3 * sd ) {
		    
		    if( outFile.is_open() ) {
			string str = purchaseObj.returnWriteString(); 

			stringstream strStream;
			strStream.precision(2);
			strStream << fixed;

			mu = trunc( mu * 100 ) / 100;
			strStream << mu;
			string muStr = strStream.str();
			strStream.str("");

			sd = trunc( sd * 100 ) / 100;
			strStream << sd ;
			string strSd = strStream.str();
			str += ", \"mean\": \""+muStr+"\", \"sd\": \""+strSd+"\"}"; 

			//Write the anomalous purchase to the output file  
			outFile<< str << "\n";
		    }
		}
	   }

	}
	
    }

    //Print the graph
    void print_graph() {
	size_t numUsers = adj.size();
	cout<<"Num of user " <<  numUsers << endl;
	for(adjMapType::iterator it = adj.begin(); it != adj.end(); ++it) {
	    cout<<"user " << it->first +1 <<"  connected to: " <<endl;

	    for(unordered_map<vid_t, bool>::iterator adjIt = it->second.begin(); adjIt != it->second.end(); ++adjIt) {
		cout<<adjIt->first +1<<" ";	 
	    }
	    cout<<endl;
	}
    }

    //Print purchases of the users
    void print_puchases() {
	size_t numUsers = userPurchases.size();
	cout<<"Num of user " <<  numUsers << endl;

	size_t numUsersBoughtManyItems = 0;
	

	for(adjPurchaseType::iterator it = userPurchases.begin(); it != userPurchases.end(); ++it) {
	    cout<<"user " << it->first +1 <<"  number of items bought : ";
	    cout<< it->second.size() << endl;
	    if( it->second.size() > T ) 
		numUsersBoughtManyItems ++;
	}
	cout<<"Number of user who bought more than T items: "<< numUsersBoughtManyItems <<" out of : "<< numUsers <<endl;
    }
   
    //Open the output file to write anomalous purchases
    void openOutFile(char *outFileName ) {
        //open the file
        outFile.open( outFileName );

	if( !outFile.is_open() ) {
	    cout<<"File can not be open: "<< outFileName << endl;
	} 
	else {
	    outFile.precision(2);
	    outFile << fixed;
	}
    }

    //Destructor -- to close the output file
    ~graph_t() {
	outFile.close();
    }
};

//Read batch_log.json and populate the graph
void buildTheNetwork(char *batchLogFile, unsigned int & D, unsigned int & T, graph_t & g);

//Read stream_log.json and write to output file and update the graph
void readStreamData(char *batchLogFile, graph_t & g);

//Parse each line of input file
void parseLine(string line, vector< vector<string> > & eventValues);

//trim a string
string trimString(string str) ;

int main(int argc, char *argv[]) {
    if( argc < 4) {
	cout<<"Usage: " << argv[0]<<" batch_log stream_log flagged_log"<<endl;
	exit(1);
    }

    
    //D -- degree of fiendship
    //T -- number of transactions
    unsigned int D = 0, T = 0;

    //define a graph object
    graph_t g;

    //Read batch_log.json and populate the graph  
    buildTheNetwork(argv[1], D, T, g);

    //Open the output file
    g.openOutFile( argv[3] );

    //Read stream_log.json and write to output file and update the graph
    readStreamData(argv[2], g);

    return 0;
}

//Read stream_log.json and check each purchase for anomaly
void readStreamData(char *batchLogFile, graph_t & g) { 
    //open the file
    ifstream inFile( batchLogFile );

    //If file exists
    if( inFile.is_open() ) {
	string line;

	while( getline(inFile, line) ) {
	    
            vector< vector<string> > eventValues; 
            parseLine(line, eventValues);

	    //Decide using the evenet type to build network	
	    if( eventValues.size() > 0 ) {
		string eventType = eventValues[0][1];

		//Two users become friends 
		if( trimString( eventType ) == "befriend" ) {
		    vid_t id1 = stoul(  trimString( eventValues[2][1] ) );
		    vid_t id2 = stoul(  trimString( eventValues[3][1] ) );
			
		    g.addFriend( id1, id2);
			
		}
		//Two users ending their friendship 
		else if( trimString(eventType) == "unfriend") { 
		    vid_t id1 = stoul(  trimString( eventValues[2][1] ) );
		    vid_t id2 = stoul(  trimString( eventValues[3][1] ) );
			
		    g.deleteFriend( id1, id2);
		}
		//A user makes a purchase
		else {
		    Purchase purchaseObj; 
		    purchaseObj.orgTimeStamp = eventValues[1][1];
		    purchaseObj.timeStamp =  trimString( eventValues[1][1] ); 

                    purchaseObj.strAmount = eventValues[3][1];
                    purchaseObj.strId = eventValues[2][1];

		    string amount =  trimString( eventValues[3][1] ) ;
		    purchaseObj.amount = stod( amount  ); 

		    string id = trimString( eventValues[2][1] );
		    purchaseObj.id = stoul(  id ); 

		    //check if the purchase is anomalous or not
		    g.addPurchaseandCheckAnomalies( purchaseObj );
		}

	    }
	}
    }
    else {
        cout<<"Can not open file: " << batchLogFile << "\nExiting: \n";
        exit( 1 );
    }
    inFile.close();
   
}

//Read batch_log.json and populate the graph 
void buildTheNetwork(char *batchLogFile, unsigned int & D, unsigned int & T, graph_t & g) {
    //open the file
    ifstream inFile( batchLogFile );

    //If file exists
    if( inFile.is_open() ) {

	//each line of input string
	string line;

        bool isThisFirstLine = true;

	while( getline(inFile, line) ) {

	    //This is the first line -- read D and T
	    if( isThisFirstLine ) {	    
		isThisFirstLine = false;

                vector< vector<string> > eventValues; 
                parseLine(line, eventValues);
	
		//First line contains D and T
		if( eventValues.size() > 0 ) {
		    D = stoul(  trimString( eventValues[0][1] ) );
		    g.D = D;
	
		    if( eventValues.size() > 1 ) {
			T = stoul( trimString( eventValues[1][1] ) );
			g.T = T;
		    } 
		} 

	    }
	    else {
                vector< vector<string> > eventValues; 
                parseLine(line, eventValues);

		//Decide using the evenet type to build network	
		if( eventValues.size() > 0 ) {
		    string eventType = eventValues[0][1];

		    //Two users become friends 
		    if( trimString( eventType ) == "befriend" ) {

			vid_t id1 = stoul(  trimString( eventValues[2][1] ) );
			vid_t id2 = stoul(  trimString( eventValues[3][1] ) );
			
			g.addFriend( id1, id2);
			
		    }
		    //Two users ending their friendship
		    else if( trimString(eventType) == "unfriend") { 
		       
			vid_t  id1 = stoul(  trimString( eventValues[2][1] ) );
			vid_t  id2 = stoul(  trimString( eventValues[3][1] ) );
			
			g.deleteFriend( id1, id2);
		    }
		    //A user makes a purchase
		    else {

			Purchase purchaseObj; 
			purchaseObj.timeStamp =  trimString(  eventValues[1][1]   ); 

			purchaseObj.strAmount = eventValues[3][1];
			purchaseObj.strId = eventValues[2][1];

			
			string amount =  trimString( eventValues[3][1] ) ;
			purchaseObj.amount = stod( amount  ); 

			string id = trimString( eventValues[2][1] );
			purchaseObj.id = stoul(  id ); 
			
			g.addPurchase( purchaseObj );
		    }

		}
	    }
	}
			
    }
    else {
	cout<<"Can not open file: " << batchLogFile << "\nExiting: \n";
	exit( 1 );
    }
    inFile.close();
 
}


//Parse a line of input
void parseLine(string line, vector< vector<string> > & eventValues) {

    size_t start = 0;
    string eventType, value;
    bool eventValDelim = true;
    size_t stringLen = line.size();

    for(size_t i = 0; i < stringLen; i++) {
	if( line[i] == '{') {
	    start = i + 1;
	}	
	else {
	    //get the event type and value
	    char ch = line[i];

	    if( eventValDelim && ch == ':' ) {
	        eventValDelim =  false;
		eventType = line.substr(start, i - start);

		start = i +1;
	    }
	    else if( ch == ',' || ch == '}') {
		value = line.substr(start, i - start);
		start = i +1;

		vector<string> eventVal(2,  "");
		eventVal[0] = eventType;
		eventVal[1] = value;

		eventValues.push_back( eventVal );
		eventValDelim = true;
	    }
	}
    }

}

//trim a string
string trimString(string str) {
    string result = "";
    
    for(size_t i = 0; i < str.size(); i++) {

	char ch = str[i];
	if( ch != '"' && ch != ' ' && ch != '-' && ch != ':' ) {
	    result += str[i];
	} 
    }
    return result;
}

//calculate mean of first N purchases in the vector
double mean(vector<Purchase>& purchases, size_t N) {
    double sum = 0;
    for(size_t i = 0; i < N; i++) {
	sum += purchases[i].amount;
    }
    return sum / N;
}


//calculate SD of first N purchases in the vector
double standardDediation(vector<Purchase>& purchases, double mu, size_t N) {
    double sumSquare = 0;
    for(size_t i = 0; i < N; i++) {
	double x = purchases[i].amount ;
	sumSquare += (x - mu) * (x - mu);
    }
    sumSquare = sumSquare / N;
    return sqrt( sumSquare );
}


