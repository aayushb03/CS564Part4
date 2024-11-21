#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.
        status = db.createFile(fileName);
        if (status != OK) printf("error in db.createFile call\n");

        // open the file & alloc an empty header page
        status = db.openFile(fileName, file);

        bufMgr->allocPage(file, hdrPageNo, newPage);

        hdrPage = (FileHdrPage *) newPage;

        strcpy(hdrPage->fileName, fileName.c_str());
        // printf("hdrPage->fileName = %s\n", hdrPage->fileName);

        hdrPage->recCnt = 0;

        // allocate an empty data page
        bufMgr->allocPage(file, newPageNo, newPage);

        newPage->init(newPageNo);

        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;

        // unpin both and mark as dirty
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);

        return (OK);
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        db.openFile(fileName, filePtr);
        // read in the header page
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);

        filePtr->getFirstPage(headerPageNo);
        headerPage = (FileHdrPage *) pagePtr;
        hdrDirtyFlag = false;

        // read in the first data page into the buffer pool
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);

        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = OK;
        cout << "opened file " << fileName << " with " << headerPage->recCnt << " records" << endl;
        return;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

//    cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    if (curPage == NULL) {
        // read the right page (the one with the required record) into the buffer
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        curPageNo = rid.pageNo;
        curDirtyFlag = true;
        curRec = rid;
//        cout << "getRecord: read correct page " << headerPage->lastPage << " into buffer pool" << endl;
    }

    // if desired record is on the current page ok, else unpin the current pinned page and use the pageNo field of the RID to read the page into the bufer pool
    if (curPageNo != rid.pageNo) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
//        cout << "getRecord: read correct page " << headerPage->lastPage << " into buffer pool" << endl;
    }

    // got correct page, now get the record
    status = curPage->getRecord(rid, rec);
//    cout << "getRecord: got record " << rid.slotNo << " from page " << rid.pageNo << endl;
    return status;
}

HeapFileScan::HeapFileScan(const string & name,
                           Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char* filter_,
                                     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;

        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;



    cout << "scan finished" << endl;
    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    // If the current page is NULL, start with the first page
    if (curPage == NULL) {
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK) return status; // Handle read failure
        curPageNo = headerPage->firstPage;

        // Get the first record on the page
        status = curPage->firstRecord(curRec);
        if (status != OK) return status; // Handle no records on the page
    } else {
        // Attempt to get the next record on the current page
        status = curPage->nextRecord(curRec, tmpRid);
        if (status == ENDOFPAGE) {
            // If end of the page is reached, move to the next page
            bufMgr->unPinPage(filePtr, curPageNo, false); // Unpin the current page

            // Get the next page number
            status = curPage->getNextPage(nextPageNo);
            if (nextPageNo == -1) return FILEEOF; // No more pages to scan

            // Read the next page into the buffer
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK) return status; // Handle read failure
            curPageNo = nextPageNo;

            // Get the first record on the new page
            status = curPage->firstRecord(curRec);
            if (status != OK) return status; // Handle no records on the page
        } else if (status != OK) {
            return status; // Handle other errors
        } else {
            curRec = tmpRid; // Update the current record RID
        }
    }

    // Retrieve the record data using the current RID
    status = curPage->getRecord(curRec, rec);
    if (status != OK) return status;

    // Check if the record matches the scan predicate
    if (matchRec(rec)) {
        outRid = curRec; // Store the matching record RID
        return OK;
    }

    // If the record does not match, recursively call scanNext to find the next match
    return scanNext(outRid);
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
        return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

        case INTEGER:
            int iattr, ifltr;                 // word-alignment problem possible
            memcpy(&iattr,
                   (char *)rec.data + offset,
                   length);
            memcpy(&ifltr,
                   filter,
                   length);
            diff = iattr - ifltr;
            break;

        case FLOAT:
            float fattr, ffltr;               // word-alignment problem possible
            memcpy(&fattr,
                   (char *)rec.data + offset,
                   length);
            memcpy(&ffltr,
                   filter,
                   length);
            diff = fattr - ffltr;
            break;

        case STRING:
            diff = strncmp((char *)rec.data + offset,
                           filter,
                           length);
            break;
    }

    switch(op) {
        case LT:  if (diff < 0.0) return true; break;
        case LTE: if (diff <= 0.0) return true; break;
        case EQ:  if (diff == 0.0) return true; break;
        case GTE: if (diff >= 0.0) return true; break;
        case GT:  if (diff > 0.0) return true; break;
        case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
    //Do nothing. Heapfile constructor will bread the header page and the first
    // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid) {
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED) {
        return INVALIDRECLEN; // Record too large
    }

    if (curPage == NULL) {
        Status status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK) return status; // Handle read failure
        curPageNo = headerPage->lastPage;
    }

    if (curPage->insertRecord(rec, outRid) == OK) {
        curDirtyFlag = true;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        return OK;
    }

    // If we cannot insert into the current page
    bufMgr->unPinPage(filePtr, curPageNo, true); // Mark as dirty

    Page* newPage;
    int newPageNo;
    Status status = bufMgr->allocPage(filePtr, newPageNo, newPage);
    if (status != OK) return status; // Allocation failed

    newPage->init(newPageNo);
    headerPage->lastPage = newPageNo;
    headerPage->pageCnt++;
    hdrDirtyFlag = true;

    curPage->setNextPage(newPageNo); // Link pages
    curDirtyFlag = true;

    curPage = newPage;
    curPageNo = newPageNo;

    if (curPage->insertRecord(rec, outRid) == OK) {
        curDirtyFlag = true;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        return OK;
    }

    return INVALIDRECLEN; // Unexpected failure
}
