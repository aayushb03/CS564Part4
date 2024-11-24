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
        // db.openFile(fileName, filePtr);
        // read in the header page
        filePtr->getFirstPage(headerPageNo);

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);

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

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    if (curPage == NULL) {
        // read the right page (the one with the required record) into the buffer
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        printf("curPage %x\n", curPage);
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
        // cout << "getRecord: read correct page " << headerPage->lastPage << " into buffer pool" << endl;
    }

    // if desired record is on the current page ok, else unpin the current pinned page and use the pageNo field of the RID to read the page into the bufer pool
    if (curPageNo != rid.pageNo) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
        // cout << "getRecord: read correct page " << headerPage->lastPage << " into buffer pool" << endl;
    }

    // got correct page, now get the record
    cout << "rid= " << curRec.pageNo << "." << curRec.slotNo << endl;
    status = curPage->getRecord(rid, rec);

    // cout << "getRecord: got record " << rid.slotNo << " from page " << rid.pageNo << endl;
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
    // if curPage null, read the next page


    if (curPage == NULL)
    {


        // read in the first data page
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);

        // page bookkeeping
        cout << "first page: " << headerPage->firstPage << endl;
        curPage->firstRecord(curRec);
        curPageNo = 1;
        curDirtyFlag = false;
        // cout << "curRec: " << curRec.pageNo << "." << curRec.slotNo << endl;
        // cout << "read first page" << endl;
    }











    
    // if (curPage == NULL)
    // {
    //     cout << "curPage is null" << endl;

    //     status = bufMgr->readPage(filePtr, 1, curPage);
    //     cout << "read first page" << endl;

    //     curPage->firstRecord(curRec);
    //     curPageNo = 1;
    //     curDirtyFlag = false;
    //     cout << "curRec: " << curRec.pageNo << "." << curRec.slotNo << endl;

    //     Record rec;
    //     status = curPage->getRecord(curRec, rec);
    //     if (status != OK) return status;
    // }
    // // cout << "curPage: " << curPage << endl;
    // // status = curPage->nextRecord(curRec, nextRid);

    // // if (status == ENDOFPAGE) {
    // //     cout << "END OF PAGE" << endl;
    // //     // get the next page
    // //     status = curPage->getNextPage(nextPageNo);
    // //     bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    // //     bufMgr->readPage(filePtr, nextPageNo, curPage);
    // //     curPageNo = nextPageNo;
    // //     curDirtyFlag = false;
    // //     curPage->firstRecord(curRec);
    // // }

    // // // convert rid to a pointer to the record data nad invoke matchRec to determine if record satisfies 
    // // // the scan predicate
    // // if (matchRec(rec)) {
    // //     outRid = curRec;
    // //     return OK;
    // // } else {
    // //     return scanNext(outRid);
    // // }

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
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // see if the current page has room
    if (curPage == NULL) {
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
    //    cout << "insertRecord: read last page " << headerPage->lastPage << " into buffer pool" << endl;
    }


    if (curPage->insertRecord(rec, rid) == OK)
    {
        outRid = rid;
        curDirtyFlag = true;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
    //    cout << "Inserted record in currPage" << endl;

        return OK;
    } else {
        // insert unsuccessful, create new page and link appropriately
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, true);
        // bufMgr->disposePage(filePtr, curPageNo);
    //    cout << "insertRecord: unpinned and disposed of page " << curPageNo << endl;
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) {
            cout << "error in allocPage call in insertRecord\n";
            return status;
        }
        newPage->init(newPageNo);
    //    cout << "insertRecord: allocated new page " << newPageNo << " into buffer pool" << endl;
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // link the new page to the current page
        curPage->setNextPage(newPageNo);
        curDirtyFlag = true;
        curPage = newPage;
        curPageNo = newPageNo;

        // insert the record into the new page
        if (curPage->insertRecord(rec, rid) == OK)
        {
            outRid = rid;
            curDirtyFlag = true;
            headerPage->recCnt++;
            hdrDirtyFlag = true;
            return OK;
        } else {
            return INVALIDRECLEN;
        }
    }
}
