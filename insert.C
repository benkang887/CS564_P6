#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{

    Status status;
    int relAttrCnt;
    AttrDesc *relAttrs;

    // Get relation info
    status = attrCat->getRelInfo(relation, relAttrCnt, relAttrs);
    if (status != OK) {
        return status;
    }

    // Calculate the length of the record
    int recLen = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        recLen += relAttrs[i].attrLen;
    }

    // Create a buffer for the record
    char recordData[recLen];
   
    for (int i = 0; i < relAttrCnt; i++) {

        int inputIndex = -1;
        // Search for this attribute
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(relAttrs[i].attrName, attrList[j].attrName) == 0) {
                inputIndex = j;
                break;
            }
        }

        char * inputVal = (char *)attrList[inputIndex].attrValue;
        int outputOffset = relAttrs[i].attrOffset;
    
        if (relAttrs[i].attrType == INTEGER) {
             int val = atoi(inputVal);
             memcpy(recordData + outputOffset, &val, sizeof(int));
        }
        else if (relAttrs[i].attrType == FLOAT) {
             float val = atof(inputVal);
             memcpy(recordData + outputOffset, &val, sizeof(float));
        }
        else if (relAttrs[i].attrType == STRING) {
             strncpy(recordData + outputOffset, inputVal, relAttrs[i].attrLen);
        }
        else {
             delete[] relAttrs;
        }
    }

    delete[] relAttrs;

    // insert the record
    InsertFileScan iScan(relation, status);
    if (status != OK) {
        return status;
}
    Record rec;
    rec.data = recordData;
    rec.length = recLen;
    RID outRid;

    status = iScan.insertRecord(rec, outRid);
    if (status != OK) {
        return status;
    }

    return OK;
}