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

	void *filter;
	int tmpInt;
	float tmpFloat;

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

	//desc for the selection attr
	AttrDesc attrDesc;
	status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
	
	if (status != OK)
		return status;

	int reclen = 0;

	//total length of output records
	for (int i = 0; i < projCnt; i++)
		reclen += attrDescArr[i].attrLen;

	return ScanSelect(result, projCnt, attrDescArr, &attrDesc, op, (char *)filter, reclen);
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
		
	HeapFileScan filterScan(string(attrDesc->relName), status);

	if (status != OK)
		return status;


	status = filterScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);

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
