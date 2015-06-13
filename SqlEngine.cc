/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <string>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include <climits>

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }
  
  BTreeIndex tree;

  //check if any condition on key
  int length = cond.size();
  bool isOnKey = false;
	for(int i = 0; i < length; i++){
	  //on key  
	  if(cond[i].attr == 1 && cond[i].comp != SelCond::NE){
	    isOnKey = true;
	    break;
	  }
	}

  //if condition on key and index exists, use index
  if(isOnKey && tree.open(table + ".idx", 'r') == 0){
    //"combine" range
    int lowerBound = 0;
    int upperBound = INT_MAX;
    int equalityVal;
    bool hasEquality = false;
    bool hasRange = false;

    for(int i = 0; i < length; i++){
		  if(cond[i].attr == 1){
		  	int value = atoi(cond[i].value);
		  	if(cond[i].comp == SelCond::GT && lowerBound < (value + 1)){
		  		lowerBound = value + 1;
		  		hasRange = true;
		  	}
		  	else if(cond[i].comp == SelCond::GE && lowerBound < value){
		  		lowerBound = value;
		  		hasRange = true;
		  	}
		  	else if(cond[i].comp == SelCond::LT && upperBound > (value - 1)){
		  		upperBound = value - 1;
		  		hasRange = true;
		  	}
		  	else if(cond[i].comp == SelCond::LE && upperBound > value){
		  		upperBound = value;
		  		hasRange = true;
		  	}

		  	//check equality
		  	if(cond[i].comp == SelCond::EQ){
		  		hasEquality = true;
		  		equalityVal = value;
		  	}
		  }
		}

		//if lowerBound > upperBound, return
		if(lowerBound > upperBound){
			tree.close();
  		rf.close();
			return 0;
		}

		//check if equlity is within range
		if(hasEquality && (equalityVal < lowerBound || equalityVal > upperBound)){
			tree.close();
  		rf.close();
			return 0;
		}

		//conditions make sense, so start query
		if(hasEquality){
      //cout << "checking eq cond" << endl;
			IndexCursor cursor;
			if(tree.locate(equalityVal, cursor) == RC_NO_SUCH_RECORD){
				tree.close();
  			rf.close();
				return 0;
			}

			int count1 = 0;
			//retrieve record from rf using rid
			int key1 = 0;
			RecordId rid;
			tree.readForward(cursor, key1, rid);
			string stringValue;
			if(rf.read(rid, key1, stringValue)){
        cout << "read error\n";
        tree.close();
        rf.close();
        return 0;
      }

			//check condition on this tuple
      //cout << checkOnTuple(attr, key1, stringValue, cond) << endl;
			if(checkOnTuple(attr, key1, stringValue, cond))
				count1++;

			if (attr == 4) {
	      fprintf(stdout, "%d\n", count1);
	    }
		}

		else if(hasRange){
			IndexCursor cursor;
			tree.locate(lowerBound, cursor);

			int currentKey;
			RecordId currentRid;
			string stringValue;
			int count2 = 0;
			int key2 = 0;
			while(tree.readForward(cursor, currentKey, currentRid) != RC_INVALID_CURSOR){
				if(currentKey > upperBound)
					break;
        if(rf.read(currentRid, key2, stringValue)){
          cout << "read error\n";
          tree.close();
          rf.close();
          return 0;
        }
				//rf.read(rid, key2, stringValue);
				if(checkOnTuple(attr, key2, stringValue, cond)){
					count2++;
				}
			}

			if (attr == 4) {
	      fprintf(stdout, "%d\n", count2);
	    }
		}

  	tree.close();
  	rf.close();
  	return 0;
	}

  else{ 
    // scan the table file from the beginning
    rid.pid = rid.sid = 0;
    count = 0;
    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
        case 1:
        	diff = key - atoi(cond[i].value);
        	break;
        case 2:
        	diff = strcmp(value.c_str(), cond[i].value);
        	break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) {
        case SelCond::EQ:
        	if (diff != 0) goto next_tuple;
        	break;
        case SelCond::NE:
        	if (diff == 0) goto next_tuple;
        	break;
        case SelCond::GT:
        	if (diff <= 0) goto next_tuple;
        	break; 
        case SelCond::LT:
        	if (diff >= 0) goto next_tuple;
        	break;
        case SelCond::GE:
        	if (diff < 0) goto next_tuple;
        	break;
        case SelCond::LE:
        	if (diff > 0) goto next_tuple;
        	break;
        }
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }

    // print matching tuple count if "select count(*)"
    if (attr == 4) {
      fprintf(stdout, "%d\n", count);
    }
    rc = 0;
  }

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
  //cout << table << " " << loadfile << endl;
  
  //open loadfile
  ifstream fsLoad;
  fsLoad.open(loadfile.c_str());
  if(!fsLoad.is_open()){
    cout << "Error opening file.\n";
    return 1;
  }

  //open or create table
  RecordFile record;
  string tableName = table + ".tbl";
  if(record.open(tableName, 'w')){
    cout << "Error opening file" << endl;
    return 1;
  }

  //read loadfile lines
  BTreeIndex tree;
  if(index){
    tree.open(table + ".idx", 'w');
  }
  string line;
  while(getline(fsLoad, line, '\n')){
    int key;
    string value;
    parseLoadLine(line, key, value);
    //cout << key << " " << value << endl;

    //insert into record file
    RecordId rid = record.endRid();
    record.append(key, value, rid);

    //insert into index
    if(index){
      tree.insert(key, rid);
    }
  }

  if(index)
    tree.close();
  fsLoad.close();
  record.close();
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}

bool checkOnTuple(int selectAttr, int key, const string& stringValue, const vector<SelCond>& cond){
	int diff;
	string value;

	// check the conditions on the tuple
  for (unsigned i = 0; i < cond.size(); i++) {
    // compute the difference between the tuple value and the condition value
    switch (cond[i].attr) {
    case 1:
    	diff = key - atoi(cond[i].value);
      cout << key << " " << atoi(cond[i].value) << " " << diff << endl;
    	break;
    case 2:
    	diff = strcmp(value.c_str(), cond[i].value);
    	break;
    }

    // skip the tuple if any condition is not met
    switch (cond[i].comp) {
    case SelCond::EQ:
    	if (diff != 0){ 
        cout << "returning false in EQ\n"; 
        return false;
      }
    case SelCond::NE:
    	if (diff == 0){ 
        cout << "returning false in NE\n"; 
        return false;
      }
    case SelCond::GT:
    	if (diff <= 0){
        cout << "returning false in GT\n"; 
        return false;
      }
    case SelCond::LT:
    	if (diff >= 0){
       cout << "returning false in LT\n"; 
       return false;
      }
    case SelCond::GE:
    	if (diff < 0) {
        cout << "returning false in GE\n"; 
        return false;
      }
    case SelCond::LE:
    	if (diff > 0) {
        cout << "returning false in LE\n"; 
        return false;
      }
    }
  }
  //fprintf(stdout, "fprintf works!\n");
  // print the tuple 
  switch (selectAttr) {
  case 1:  // SELECT key
    fprintf(stdout, "%d\n", key);
    //cout << key << endl;
    break;
  case 2:  // SELECT value
    fprintf(stdout, "%s\n", stringValue.c_str());
    //cout << stringValue << endl;
    break;
  case 3:  // SELECT *
    cout << "in * case" << endl;
    fprintf(stdout, "%d '%s'\n", key, stringValue.c_str());
    //cout << key << "'" << stringValue << "'\n";
    break;
  }

  return true;
}

