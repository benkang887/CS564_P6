#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	Status status;

	AttrDesc attrDescArr[projCnt];

	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArr[i]);
		if (status != OK)
			return status;
	}

	//desc for the selection attr
	AttrDesc attrDesc;
    AttrDesc *attrDescPtr = NULL;
    void *filter;
    int tmpInt;
    float tmpFloat;

    if (attr != NULL) {
        cout << "attr" << attr << endl;
        switch (attr->attrType) {
            case INTEGER:
                tmpInt = atoi(attrValue);
                filter = (void *) &tmpInt;	
                break;
            case FLOAT:
                tmpFloat = atof(attrValue);
                filter = (void *) &tmpFloat;	
                break;
            case STRING:
                filter = (void *) attrValue;
                break;
        }

        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        
        if (status != OK)
            return status;
            
        attrDescPtr = &attrDesc;
    } else {
        filter = NULL;
    }

	int reclen = 0;

	//total length of output records
	for (int i = 0; i < projCnt; i++)
		reclen += attrDescArr[i].attrLen;

	return ScanSelect(result, projCnt, attrDescArr, attrDescPtr, op, (char *)filter, reclen);
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	
	InsertFileScan resultRel(result, status);

	if (status != OK)
		return status;

	char outputData[reclen];
	Record outputRec;
	outputRec.data = (void*) outputData;
	outputRec.length = reclen;
		
    string relName;
    if (attrDesc) {
        relName = string(attrDesc->relName);
    } else {
        relName = string(projNames[0].relName);
    }
    
	HeapFileScan filterScan(relName, status);

	if (status != OK)
		return status;


    if (attrDesc) {
	    status = filterScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    } else {
        status = filterScan.startScan(0, 0, STRING, NULL, EQ);
    }

	RID filteredRID;
	Record filteredRec;

	while (filterScan.scanNext(filteredRID) == OK) {
		status = filterScan.getRecord(filteredRec);
		ASSERT(status == OK);

		//project otf
		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++) {
				memcpy(outputData + outputOffset, (char *)filteredRec.data + projNames[i].attrOffset, projNames[i].attrLen);
				outputOffset += projNames[i].attrLen;
		}


		//insert into output
		RID outRID;
		status = resultRel.insertRecord(outputRec, outRID);
		ASSERT(status == OK);
		
		

	}

	return OK;

}
