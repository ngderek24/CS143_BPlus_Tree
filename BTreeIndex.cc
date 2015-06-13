/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	rootPid = 1;
	treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	int pfResult = pf.open(indexname, mode);	

	pf.open(indexname, 'w');
	char buf[1024];
	if(!pf.endPid()){
    	rootPid = 1;
    	treeHeight = 0;
    	memcpy(buf, (void*)&rootPid, sizeof(rootPid));
    	memcpy(buf + 4, (void*)&treeHeight, sizeof(treeHeight));
    	pfResult = pf.write(0, buf);
    }
    else{
    	pf.read(0, buf);
    	memcpy(&rootPid, (void*)buf, sizeof(rootPid));
    	memcpy(&treeHeight, (void*)(buf + 4), sizeof(treeHeight));
    }

    return pfResult;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	char buf[1024];
	memcpy(buf, (void*)&rootPid, sizeof(rootPid));
    memcpy(buf + 4, (void*)&treeHeight, sizeof(treeHeight));
	pf.write(0, buf);
    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	BTNonLeafNode root;
	if(treeHeight == 0){
		RecordId rid1, rid2;
		rid1.pid = pf.endPid() + 1;
		rid2.pid = pf.endPid() + 2;
		root.initializeRoot(rid1, key, rid2);
		treeHeight += 2;

		//write root and 2 leaves
		root.write(rootPid, pf);
		BTLeafNode leaf1, leaf2;
		leaf2.insert(key, rid);
		leaf1.setNextNodePtr(rid2.pid);
		leaf1.write(rid1.pid, pf);
		leaf2.write(rid2.pid, pf);

		return 0;
	}
	else{
		root.read(rootPid, pf);
	}
	RecordId rid3;
	root.locateChildPtr(key, rid3);
	int prevResult = insertHelper(key, rid, rid3.pid);

	if(prevResult == OVF){
		//ovf from level below, insert to root
		int currentResult = root.insert(key, rid3);
		int originalRootPid = rootPid;
		if(currentResult == RC_NODE_FULL){
			//root is full, so we must create a sibling and a new root
			BTNonLeafNode sibling;
			int midKey;
			root.insertAndSplit(key, rid3, sibling, midKey);

			//write nodes to disk
			PageId siblingPid = pf.endPid();
			sibling.write(siblingPid, pf);

			//create new root
			BTNonLeafNode newRoot;
			RecordId tempR1, tempR2;
			tempR1.pid = rootPid;
			tempR2.pid = siblingPid;
			newRoot.initializeRoot(tempR1, midKey, tempR2);
			treeHeight++;

			//write new root to disk
			PageId newRootPid = pf.endPid();
			rootPid = newRootPid;
			newRoot.write(newRootPid, pf);
		}
		root.write(originalRootPid, pf);
	}

    if(prevResult == RC_FILE_READ_FAILED || 
    	prevResult == RC_INVALID_CURSOR)
    	return prevResult;
    return 0;
}

RC BTreeIndex::insertHelper(int& key, const RecordId& rid, PageId& pid){
	//read current pid into node
	PageId originalPid = pid;
	BTNonLeafNode node;
	if(node.read(originalPid, pf))
		return RC_FILE_READ_FAILED;

	//if leaf
	if(node.getBufferChar(1015) == 'L'){
		BTLeafNode leaf;
		if(leaf.read(originalPid, pf))
			return RC_FILE_READ_FAILED;
		int inputKey = key;
		int leafResult = leaf.insert(key, rid);

		//handle ovf
		if(leafResult == RC_NODE_FULL){
			BTLeafNode sibling;
			int sibkey;
			leaf.insertAndSplit(key, rid, sibling, sibkey);
			
			//set next ptr
			PageId siblingPid = pf.endPid();
			sibling.setNextNodePtr(leaf.getNextNodePtr());
			leaf.setNextNodePtr(siblingPid);
			
			//write sibling to disk			
			sibling.write(siblingPid, pf);

			//change parameters for parent
			key = sibkey;
			pid = siblingPid;
			leafResult = OVF;
		}
		leaf.write(originalPid, pf);
		return leafResult;
	}
	//if nonleaf
	else{
		//find the child ptr to follow
		RecordId r;
		if(node.locateChildPtr(key, r))
			return RC_INVALID_CURSOR;

		//insert recursively
		int prevResult = insertHelper(key, rid, r.pid);
		if(prevResult == RC_INVALID_CURSOR)
			return RC_INVALID_CURSOR;

		//handle ovf
		int currentResult = prevResult;
		if(prevResult == OVF){
			//child level overflowed, so try inserting into this level
			currentResult = node.insert(key, r);

			if(currentResult == RC_NODE_FULL){
				BTNonLeafNode sibling;
				int midKey;
				node.insertAndSplit(key, r, sibling, midKey);

				//write nodes to disk
				PageId siblingPid = pf.endPid();
				sibling.write(siblingPid, pf);

				//change parameters for parent
				key = midKey;
				pid = siblingPid;
				currentResult = OVF;
			}
		}
		node.write(originalPid, pf);
		return currentResult;
	}
	return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	BTNonLeafNode node;
	node.read(rootPid, pf);
	RecordId rid;
	while(1){
		if(node.getBufferChar(1015) == 'L')
			break;
		node.locateChildPtr(searchKey, rid);
		node.read(rid.pid, pf);
	}
	BTLeafNode leaf;
	leaf.read(rid.pid, pf);
	int eid;
	int result = leaf.locate(searchKey, eid);
	cursor.pid = rid.pid;
	cursor.eid = eid;
    return result;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	if(cursor.pid == 0)
		return RC_INVALID_CURSOR;

	BTLeafNode node;
	node.read(cursor.pid, pf);
	int result = node.readEntry(cursor.eid, key, rid);
	int keyCount = node.getKeyCount();
	if(!result){
		if(cursor.eid == keyCount - 1){
			cursor.pid = node.getNextNodePtr();
			cursor.eid = 0;
		}
		else{
			cursor.eid++;
		}
	}
    return result;
}

void BTreeIndex::printTree(){
	BTNonLeafNode node;
	node.read(rootPid, pf);

	BTNonLeafNode test;
	
	for(int i = 2; i <= 2; i++){
		test.read(i, pf);
		test.printNode();
	}
	cout << treeHeight << " " << rootPid << endl;
}
