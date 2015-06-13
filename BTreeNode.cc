#include "BTreeNode.h"
#include "PageFile.h"
#include <cstring>
#include <iostream>

using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	int result = pf.read(pid, buffer);
	int p;
	keyCount = 0;

	if(!result){
		/*
		for(int i = 0; i < N; i++){
			memcpy(&p, (void*)(buffer + i * 12), sizeof(p));
			if(p == 0)
				break;
			else
				keyCount++;
		}*/
		memcpy(&keyCount, (void*)(buffer + 1008), sizeof(keyCount));
	}
	return result;
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	memcpy(buffer + 1008, (void*)&keyCount, sizeof(keyCount));
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	return keyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	//if full, return error
	if(keyCount == N - 1)
		return RC_NODE_FULL;

	//find the spot to insert the new key
	int compKey;
	int i;
	for(i = 0; i < keyCount; i++){
		char* src = buffer + (i * 12);
		memcpy(&compKey, (void*)(src + 8), sizeof(compKey));
		if(key <= compKey){
			//found the spot, shift the rest to the right
			memmove(src + 12, (void*)src, (keyCount - i) * 12);
			break;
		}
	}

	//insert the new tuple
	memcpy(buffer + (i * 12), (void*)&rid, sizeof(rid));
	memcpy(buffer + (i * 12) + 8, (void*)&key, sizeof(key));

	keyCount++;
	return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	//first, insert into this node
	RC result = this->insert(key, rid);
	int halfKeyCount = keyCount/2;
	int cut = halfKeyCount;
	char whichNode;
	if(halfKeyCount%2 == 1){
		int comparator;
		memcpy(&comparator, (void*)(buffer + (12 * halfKeyCount) + 8), sizeof(comparator));
		if(comparator < key){
			cut = halfKeyCount + 1;
			whichNode = 'R';
		}
		else
			whichNode = 'L';
	}

	//move the right half of the node to sibling	
	memcpy(sibling.buffer, (void*)(buffer + (cut * 12)), (keyCount - cut) * 12);
	sibling.keyCount = keyCount - cut;
	keyCount = cut;	

	if(result == RC_NODE_FULL){
		//determine which node new key goes into
		if(whichNode == 'L')
			this->insert(key, rid);
		else
			sibling.insert(key, rid);
	}
	memcpy(&siblingKey, (void*)(sibling.buffer + 8), sizeof(siblingKey));
	return 0;

	//TODO: error? pid of sibling?
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid){ 
    int comparator;
    //traverse each search key to find match
    for(int i = 0; i < keyCount; i++){
        char* src = buffer + (i * 12);
        memcpy(&comparator, (void*)(src + 8), sizeof(comparator));
        //cout << comparator << " ";
        if(comparator == searchKey){
            eid = i;
            return 0;
        }
        else if(comparator < searchKey)
            eid = i+1;
        else
            break;
    }
    return RC_NO_SUCH_RECORD;

    //TODO: check if eid is N
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid){
    if(eid < 0 || eid > keyCount-1)
        return RC_INVALID_CURSOR; 
    memcpy(&key, (void*)(buffer + (eid * 12) + 8), sizeof(key));
    memcpy(&rid, (void*)(buffer + (eid * 12)), sizeof(RecordId));
    return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr(){ 
    int pageId;
    memcpy(&pageId, (void*)(buffer + 1016), sizeof(pageId));
    return pageId; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid){
    if(pid < 0)
        return RC_INVALID_PID; 
    memcpy(buffer + 1016, &pid, sizeof(pid));
    return 0;
}

//constructor
BTLeafNode::BTLeafNode(){
	keyCount = 0;
	memset(buffer, 0, 1024);
	buffer[1015] = 'L';
}

//print content of the node
void BTLeafNode::printNode(){
	int key;
	for(int i = 0; i < 3*keyCount; i++){
		memcpy(&key, (void*)(buffer + 4*i), 4);
		cout << key << " ";
		if((i%3) == 2)
			cout << "| ";
	}
	cout << endl;
}

char BTLeafNode::getBufferChar(int index){
	if(index < 0 || index > 1023)
		return 'E';
	return buffer[index];
}


/*-------------------------NON-LEAF NODE--------------------------------*/


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	int result = pf.read(pid, buffer);
	int p;
	keyCount = 0;
	
	if(!result){
		/*
		for(int i = 0; i < N; i++){
			memcpy(&p, (void*)(buffer + i * 12), sizeof(p));
			if(p == 0)
				break;
			else
				keyCount++;
		}
		*/
		memcpy(&keyCount, (void*)(buffer + 1008), sizeof(keyCount));
	}
	return result;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	memcpy(buffer + 1008, (void*)&keyCount, sizeof(keyCount));
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
	return keyCount; 
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, const RecordId& rid)
{ 
	//TODO: pointers probably not right. Check!

	//if full, return error
	if(keyCount == N - 1)
		return RC_NODE_FULL;

	//find the spot to insert the new key
	int compKey;
	int i;
	for(i = 0; i < keyCount; i++){
		char* src = buffer + (i * 12);
		memcpy(&compKey, (void*)(src + 8), sizeof(compKey));
		if(key <= compKey){
			//found the spot, shift the rest to the right
			memmove(src + 20, (void*)(src + 8), (keyCount - i) * 12);
			break;
		}
	}

	//insert the new tuple
	memcpy(buffer + (i * 12) + 12, (void*)&rid, sizeof(rid));
	memcpy(buffer + (i * 12) + 8, (void*)&key, sizeof(key));

	keyCount++;
	return 0; 
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, const RecordId& rid, BTNonLeafNode& sibling, int& midKey)
{ 
	//first, insert into this node
	RC result = this->insert(key, rid);
	int halfKeyCount = keyCount/2 + 1;

	//move the right half of the node to sibling	
	memcpy(sibling.buffer, (void*)(buffer + (halfKeyCount * 12)), (keyCount - halfKeyCount) * 12 + 8);
	sibling.keyCount = keyCount - halfKeyCount;
	keyCount = halfKeyCount - 1;
	memcpy(&midKey, (void*)(buffer + ((halfKeyCount - 1)* 12) + 8), sizeof(midKey));

	if(result == RC_NODE_FULL){
		//determine which node new key goes into
		if(key < midKey)
			this->insert(key, rid);
		else
			sibling.insert(key, rid);
	}
	return 0;
	//TODO: error? pid of sibling?
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, RecordId& rid)
{ 
	int comparator;
    //traverse each search key to find match
    //check the first key
    memcpy(&comparator, (void*)(buffer + 8), sizeof(comparator));
    if(searchKey < comparator){
    	memcpy(&rid.pid, (void*)buffer, sizeof(rid.pid));
        return 0;
    }

    //check last key
    memcpy(&comparator, (void*)(buffer + (keyCount - 1) * 12 + 8), sizeof(comparator));
	if(searchKey >= comparator){
    	memcpy(&rid.pid, (void*)(buffer + (keyCount) * 12), sizeof(rid.pid));
        return 0;
    }

    //check the rest in the middle
    int comparator2;
    for(int i = 0; i < keyCount - 1; i++){
        char* src = buffer + (i * 12);
        memcpy(&comparator, (void*)(src + 8), sizeof(comparator));
        memcpy(&comparator2, (void*)(src + 20), sizeof(comparator2));
        if(comparator <= searchKey && searchKey < comparator2){
            memcpy(&rid.pid, (void*)(src + 12), sizeof(rid.pid));
            return 0;
        }
    }
    return RC_INVALID_CURSOR;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(RecordId rid1, int key, RecordId rid2)
{ 
	memcpy(buffer, (void*)&rid1.pid, sizeof(rid1.pid));
	memcpy(buffer + 8, (void*)&key, sizeof(key));
	memcpy(buffer + 12, (void*)&rid2.pid, sizeof(rid2.pid));
	keyCount = 1;
	return 0;

	//TODO: error?
}

//print content of the node
void BTNonLeafNode::printNode(){
	int key;
	for(int i = 0; i < 3*keyCount + 1; i++){
		memcpy(&key, (void*)(buffer + 4*i), 4);
		cout << key << " ";
		if((i%3) == 2)
			cout << "| ";
	}
	cout << endl;
}

//constructor
BTNonLeafNode::BTNonLeafNode(){
	keyCount = 0;
	memset(buffer, 0, 1024);
	buffer[1015] = 'N';
}

char BTNonLeafNode::getBufferChar(int index){
	if(index < 0 || index > 1023)
		return 'E';
	return buffer[index];
}
