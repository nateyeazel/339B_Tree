#include <assert.h>
#include <iostream>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) :
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize,
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique)
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) {
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) {
      return rc;
    }

    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) {
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) {
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;

      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock

  return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  rc= b.Unserialize(buffercache,node);
  // cout << "\n Now looking at node " << node << "\n";
  // cout << "\nCalling lookuporupdate now with " << op << "and nodetype " << b.info.nodetype << "and numkeys " << b.info.numkeys << "\n";
  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	     // OK, so we now have the first key that's larger
	     // so we ned to recurse on the ptr immediately previous to
	     // this one, if it exists
	     rc=b.GetPtr(offset,ptr);

	      if (rc) { return rc; }
	       return LookupOrUpdateInternal(ptr,op,key,value);
      }  else if (key == testkey) {
          offset++;
          rc=b.GetPtr(offset,ptr);

          if (rc) { return rc; }
         return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) {
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) {
	if (op==BTREE_OP_LOOKUP) {
	  return b.GetVal(offset,value);
	} else {
	  //This should be the update code
    rc = b.SetVal(offset, value);
    if (rc){ return rc; }
    return b.Serialize(buffercache, node);
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) {
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) {
      } else {
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) {
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) {
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) {
      if (offset==0) {
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) {
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) {
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) {
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) {
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) {
    os << "\" ]";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

SIZE_T BTreeIndex::NumSlots(const SIZE_T &node){
  ERROR_T rc;
  BTreeNode b;

  rc = b.Unserialize(buffercache, node);
  // cout << "\n In numslots nodetype " << b.info.nodetype << "\n";
  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      return b.info.GetNumSlotsAsInterior();
    case BTREE_LEAF_NODE:
      // cout << "\nNumSlots went to leaf (GOOD)\n";
      return b.info.GetNumSlotsAsLeaf();
    default:
      // cout << "\nNumSlots went to default (BAD)\n";
      return 0;
    }
}

bool BTreeIndex::IsFull(const SIZE_T &node)
{
  ERROR_T rc;
  BTreeNode b;

  rc = b.Unserialize(buffercache, node);
  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      if(b.info.GetNumSlotsAsInterior() == b.info.numkeys){
        return true;
      } else{
        return false;
      }
    case BTREE_LEAF_NODE:
      if(b.info.GetNumSlotsAsLeaf() == b.info.numkeys){
        return true;
      } else{
        return false;
      }
    default:
      return false;
  }
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  SIZE_T root = superblock.info.rootnode;
  ERROR_T rc;
  KeyValuePair keyVal(key, value);
  BTreeNode rootnode;

  rc = rootnode.Unserialize(buffercache, root);
  if (rc) {  return rc; }

  if(IsFull(root)){
    // cout << "\n ROOT IS FULL!!!!!!\n";

    SIZE_T newRootNodeAddress;
    BTreeNode newrootnode(BTREE_ROOT_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());

    rc = AllocateNode(newRootNodeAddress);
    if (rc) {  return rc; }
    // cout << "\n Allocated new root at address " << newRootNodeAddress << "\n";
    newrootnode.info.rootnode = newRootNodeAddress;
    superblock.info.rootnode = newRootNodeAddress;

    rc = superblock.Serialize(buffercache, superblock_index);
    if (rc) {  return rc; }

    newrootnode.info.numkeys=0;
    rootnode.info.nodetype = BTREE_INTERIOR_NODE;
    rootnode.info.rootnode = newRootNodeAddress;

    rc = rootnode.Serialize(buffercache, root);
    if (rc) {  return rc; }
    rc = newrootnode.SetPtr(0, root);
    if (rc) {  return rc; }
    rc = newrootnode.Serialize(buffercache, newRootNodeAddress);
    if (rc) {  return rc; }

    // cout << "\n Going into splitting child and old root pointer is " << root << '\n';
    rc = SplitChild(newRootNodeAddress, 0);
    if (rc) {  return rc; }

    return InsertNonFull(newRootNodeAddress, keyVal);
  } else {


    if(rootnode.info.numkeys == 0){
      SIZE_T firstChildAddress;
      SIZE_T secondChildAddress;
      BTreeNode firstchildnode(BTREE_LEAF_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
      rc = AllocateNode(firstChildAddress);
      if (rc) {  return rc; }

      firstchildnode.info.numkeys = 0;
      BTreeNode secondchildnode(BTREE_LEAF_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
      rc = AllocateNode(secondChildAddress);
      if (rc) {  return rc; }

      secondchildnode.info.numkeys = 0;
      rootnode.info.numkeys++;
      rc = rootnode.SetPtr(0, firstChildAddress);
      if (rc) {  return rc; }
      rc = rootnode.SetPtr(1, secondChildAddress);
      if (rc) {  return rc; }
      rootnode.info.numkeys--;
      rc = firstchildnode.Serialize(buffercache, firstChildAddress);
      if (rc) {  return rc; }
      rc = secondchildnode.Serialize(buffercache, secondChildAddress);
      if (rc) {  return rc; }
      rc = rootnode.Serialize(buffercache, root);
      if (rc) {  return rc; }
      // cout << "\nCreated first child at address " << firstChildAddress << "\n";
    }

    return InsertNonFull(root, keyVal);

  }
}

ERROR_T BTreeIndex::SplitChild(const SIZE_T &parentAddress, const SIZE_T i){
  ERROR_T rc;
  BTreeNode parent;
  BTreeNode leftChild;
  SIZE_T leftChildAddress;
  // cout << "\nAt beginning of split with parent " << parentAddress << "\n";
  rc = parent.Unserialize(buffercache, parentAddress);
  if (rc) {  return rc; }
  rc = parent.GetPtr(i, leftChildAddress);
  if (rc) {  return rc; }
  rc = leftChild.Unserialize(buffercache, leftChildAddress);
  if (rc) {  return rc; }
  // cout << "\nThrough first Unserialize with child address " << leftChildAddress << "\n";

  SIZE_T rightChildAddress;
  BTreeNode rightChild(BTREE_INTERIOR_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
  rc = AllocateNode(rightChildAddress);
  if (rc) {  return rc; }
  // cout << "\n right child created with address " << rightChildAddress << "\n";

  rightChild.info.nodetype = leftChild.info.nodetype;
  rightChild.info.numkeys = (NumSlots(leftChildAddress) + 1)/ 2; // add one in case we are splitting an odd number

  KEY_T currentKeyVal;
  VALUE_T currentVal;
  // cout << "\nAt first for loop with numkeys " << rightChild.info.numkeys << "\n";
  for(SIZE_T j = 0; j < rightChild.info.numkeys; j++){
    rc = leftChild.GetKey(j + rightChild.info.numkeys, currentKeyVal);
    if (rc) {  return rc; }
    rc = rightChild.SetKey(j, currentKeyVal);
    if (rc) {  return rc; }
    if(leftChild.info.nodetype == BTREE_LEAF_NODE){
      rc = leftChild.GetVal(j + rightChild.info.numkeys, currentVal);
      if (rc) {  return rc; }
      rc = rightChild.SetVal(j, currentVal);
      if (rc) {  return rc; }
    }
  }

  SIZE_T currentPtr;
  if(leftChild.info.nodetype != BTREE_LEAF_NODE){ //If you're not at a leaf node copy the pointers
    for(SIZE_T j = 0; j <= rightChild.info.numkeys; j++){
      rc = leftChild.GetPtr(j + rightChild.info.numkeys, currentPtr);
      if (rc) {  return rc; }
      rc = rightChild.SetPtr(j, currentPtr);
      if (rc) {  return rc; }
    }
  }
  KEY_T promotedKey;
  rc = rightChild.GetKey(0, promotedKey);
  if (rc) {  return rc; }
  parent.info.numkeys++;

  leftChild.info.numkeys = NumSlots(leftChildAddress) / 2;

  for(SIZE_T j = parent.info.numkeys - 1; j > i; j--){ //Change all of the pointers of the parent to the correct place
    rc = parent.GetPtr(j, currentPtr);
    if (rc) {  return rc; }
    rc = parent.SetPtr(j+1, currentPtr);
    if (rc) {  return rc; }
  }
  // cout << "\nMade it past second for loop\n";
  rc = parent.SetPtr(i+1, rightChildAddress);
  if (rc) {  return rc; }

  // cout << "\n Parent numkeys is " << parent.info.numkeys << " and i is " << i << "\n";
  for(SIZE_T j = parent.info.numkeys - 1; j >= i; j--){ //Then change all of the keys of the parent to the correct place
        if(j == 0){
          break;
        }
        rc = parent.GetKey(j-1, currentKeyVal);
        if (rc) {  return rc; }
        rc = parent.SetKey(j, currentKeyVal);
        if (rc) {  return rc; }
  }
  // cout << "\nMade it past third for loop\n";
  parent.SetKey(i, promotedKey);

  rc=parent.Serialize(buffercache, parentAddress);
  if (rc) {  return rc; }
  rc=leftChild.Serialize(buffercache, leftChildAddress);
  if (rc) {  return rc; }
  rc=rightChild.Serialize(buffercache, rightChildAddress);
  if (rc) {  return rc; }

  return rc;
}

ERROR_T BTreeIndex::InsertNonFull(const SIZE_T &node, const KeyValuePair &KeyVal) {
    BTreeNode target;
    ERROR_T rc = target.Unserialize(buffercache, node);
    if (rc) {  return rc; }

    //Root node first insertion
    if(target.info.numkeys == 0 && target.info.nodetype == BTREE_ROOT_NODE){
      //cout << "\n Key is " << KeyVal.key << "\n";
      target.info.numkeys++;
      rc = target.SetKey(0, KeyVal.key);
      if (rc) {  return rc; }
      SIZE_T secondChildAddress;
      if (rc) {  return rc; }
      rc = target.GetPtr(1, secondChildAddress);
      //cout << "\n secondChildAddress should be " << secondChildAddress << "\n";
      if (rc) {  return rc; }
      rc = target.Serialize(buffercache, node);
      if (rc) {  return rc; }
      return InsertNonFull(secondChildAddress, KeyVal);
    } else if (target.info.numkeys == 0){ //If inserting into first child for first time.
      target.info.numkeys += 1;
      rc = target.SetKeyVal(0, KeyVal);
      if (rc) {  return rc; }
      rc = target.Serialize(buffercache, node);
      return rc;
    }



    SIZE_T num = target.info.numkeys - 1;
    KEY_T k;
    // cout << "\n NUMBER OF KEYS IN NODE " << node << " IS " << num + 1 <<"\n";
    rc = target.GetKey(num, k);
    if (rc) {  return rc; }
    if (target.info.nodetype == BTREE_LEAF_NODE)
    {
        target.info.numkeys++;
        while (num>0 && KeyVal.key < k)
        {
          // cout << "\n Inside While loop with num = " << num;
            KeyValuePair p;
            rc = target.GetKeyVal(num, p);
            if (rc) {  return rc; }
            rc = target.SetKeyVal(num+1, p);
            if (rc) {  return rc; }
            num--;
            rc = target.GetKey(num, k);
            if (rc) {  return rc; }
        }

        if(KeyVal.key == k){ //If the key already exists in the tree
          num++;
          while (num<target.info.numkeys-1) //Move everything back down NEEDS TO BE -1!!!!!
          {
            // cout << "\n Inside While loop with num = " << num;
              KeyValuePair p;
              rc = target.GetKeyVal(num+1, p);
              if (rc) {  return rc; }
              rc = target.SetKeyVal(num, p);
              if (rc) {  return rc; }
              num++;
          }
          target.info.numkeys--;
          return ERROR_CONFLICT;
        }

        if (num == 0 && KeyVal.key < k){
            KeyValuePair p;
            rc = target.GetKeyVal(0, p);
            if (rc) {  return rc; }
            rc = target.SetKeyVal(1, p);
            if (rc) {  return rc; }
        } else {
            num++;
        }

        // cout <<"\n outside while loop";
        rc = target.SetKeyVal(num, KeyVal);
        if (rc) {  return rc; }
        rc=target.Serialize(buffercache, node);
        if (rc) {  return rc; }
    }
    else
    {
        // cout << "\nInside insert non full with node type " << target.info.nodetype << " and num " << num << "\n";
        while (num > 0 && KeyVal.key < k)
        {
            // cout << "\nInside loop with num " << num << "\n";
            num--;
            rc = target.GetKey(num, k);
            if (rc) {  return rc; }
        }

        if(num == 0){ //To differentiate between getting 0th child and 1st child
            rc = target.GetKey(0, k);
            if (rc) {  return rc; }
        }
        //RECENT CHANGE
        if(k < KeyVal.key){
          num++;
        } else if(KeyVal.key == k){
          return ERROR_CONFLICT;
        }


        BTreeNode child;
        SIZE_T childAddress;

        rc = target.GetPtr(num, childAddress);
        if (rc) {  return rc; }
        rc= child.Unserialize(buffercache, childAddress);
        if (rc) {  return rc; }
        if (IsFull(childAddress))
        {
            rc = SplitChild(node, num);
            if (rc) {  return rc; }
            ERROR_T rc = target.Unserialize(buffercache, node);
            if (rc) {  return rc; }

            rc = target.GetKey(num, k);

            if (k < KeyVal.key || k == KeyVal.key)
            {
                num++;
                rc = target.GetPtr(num, childAddress);
                if (rc) {  return rc; }
                rc= child.Unserialize(buffercache, childAddress);
                if (rc) {  return rc; }
            }
        }
        // cout << "\nMade it to next InsertNonFull call with child address " << childAddress << "\n";
        rc = InsertNonFull(childAddress, KeyVal);
        if (rc) {  return rc; }
        // if(num > 0){
        //   rc = target.SetKey(num-1, k);
        //   if (rc) {  return rc; }
        // }



    }
    return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  if(key.length != superblock.info.keysize || value.length != superblock.info.valuesize){
    return ERROR_SIZE;
  } else{
    return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, (VALUE_T&)value);
  }
}


ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit
  //
  //
  return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);

  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) {
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) {
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) {
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) {
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) {
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) {
    o << "}\n";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::IsInOrder(const SIZE_T &nodeaddress, const KEY_T &minBound, const KEY_T &maxBound) const
{
    ERROR_T rc;
    BTreeNode node;
    rc = node.Unserialize(buffercache, nodeaddress);
    if (rc) {  return rc; }
    
    KEY_T lesserKeyVal;
    KEY_T greaterKeyVal;
    
    //Check the 1st key is equal to min bound, if there is one
    if (KEY_MIN < minBound and node.info.numkeys > 0) {
        rc = node.GetKey(0, lesserKeyVal);
        if (rc) {  return rc; }
        if (!(lesserKeyVal == minBound)) {  return ERROR_BADCONFIG;  }
    }
    
    //Check the last key is less than the max bound, if there is one
    if (maxBound < KEY_MAX and node.info.numkeys > 0) {
        rc = node.GetKey((node.info.numkeys - 1), greaterKeyVal);
        if (rc) {  return rc; }
        if (!(greaterKeyVal < maxBound)) {  return ERROR_BADCONFIG;  }
    }
    
    //Check that all keys are in order
    for(SIZE_T i = 0; i < node.info.numkeys - 1; i++){
        rc = node.GetKey(i, lesserKeyVal);
        if (rc) {  return rc; }
        rc = node.GetKey(i, greaterKeyVal);
        if (rc) {  return rc; }
        if (!(lesserKeyVal < greaterKeyVal)) {  return ERROR_BADCONFIG;  }
    }
    
    //Recurse on each child pointer if not leaf
    if (node.info.nodetype != BTREE_LEAF_NODE) {
        SIZE_T childAddress;
        for(SIZE_T i = 0; i <= node.info.numkeys; i++){
            // Get point
            rc = node.GetPtr(i, childAddress);
            if (rc) {  return rc; }
            // Get min bound
            if (i == 0) {
                lesserKeyVal = KEY_MIN;
            } else {
                rc = node.GetKey(i-1, lesserKeyVal);
                if (rc) {  return rc; }
            }
            // Get max bound
            if (i == node.info.numkeys) {
                greaterKeyVal = KEY_MAX;
            } else {
                rc = node.GetKey(i, greaterKeyVal);
                if (rc) {  return rc; }
            }
            // Recurse
            rc = IsInOrder(childAddress, lesserKeyVal, greaterKeyVal);
            if (rc) {  return rc; }
        }
    }
    
    // All good if we reached this point!
    return ERROR_NOERROR;
}

ERROR_T BTreeIndex::SanityCheck() const
{
    SIZE_T root = superblock.info.rootnode;
    return IsInOrder(root, KEY_MIN, KEY_MAX);
}

ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




