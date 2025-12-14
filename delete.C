#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
	Status status;
	AttrDesc attrDesc;

	status = attrCat->getInfo(relation, attrName, attrDesc);
	// Create scanner on the relation
	HeapFileScan scanner(relation, status);
	if (status != OK) { return status; };

	const char *filter;
 	switch (type) {
            case INTEGER:{
                int tmpInt = atoi(attrValue);
                filter = (char *) &tmpInt;
                break;
	    }
            case FLOAT:{
                float tmpFloat = atof(attrValue);
                filter = (char *) &tmpFloat;
                break;
	    }
            case STRING:{
                filter = attrValue;
                break;
	    }
        }	

	// Initiate scan
	status = scanner.startScan(attrDesc.attrOffset,attrDesc.attrLen,type,filter,op);
	if (status != OK) { return status; }
	
	RID dummy;
	while (scanner.scanNext(dummy) == OK){
		scanner.deleteRecord();
	}
	return OK;
}


