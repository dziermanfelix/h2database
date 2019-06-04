/**
 *
 * Maxence Weyrich
 *
 */

package org.h2.index;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.engine.Session;
import org.h2.store.Data;
import org.h2.store.Page;

import java.util.List;

/**
 * A linear hash page that contains hash index data. Format:
 * <ul>
 * <li>page type: byte</li> †
 * <li>checksum: short</li> †
 * <li>overflow page (0 if none): int</li> †
 * <li>index id: varInt</li> †
 * <li>next page id (0 if none): int</li>
 * <li>entry count: short</li>
 * <li>_list_ of offsets: short</li>
 * <li>data (key: varLong, value,...)</li>
 * </ul>
 *
 * † these fields (with those types and that order) are mandatory as per the PageStore implementation
 */
public class PagePersistentHash extends Page {

    /**
     * The size of each offset entry
     */
    private static final int OFFSET_LENGTH = 2;

    private static final int NO_PAGE = 0;

    /**
     * The type of the page
     */
    private int pageType;

    /**
     * The index.
     */
    private PagePersistentHashIndex index;

    /**
     * The ID of the next page in the list
     */
    private int nextPageId;

    /**
     * The page ID
     */
    private int pageId;

    /**
     * The overflow PageID
     */
    private int overflowPageId;

    /**
     * Data object to organize the data for writing Page to disk
     * Use this to represent & store the raw data before it gets written to disk
     */
    /*
    A note: the data object is used to store/organize the data before it gets written to disk.
    As data is pushed to the object, it keeps track of how much data /space it needs and calculates
    the offset.

    Reset just moves that offset tracker back to 0 so that all new data pushed to the object
    replaces whatever was there before.

    Once all the data is pushed, it is actually written to disk using the
    'index.getPageStore.writePage(getPos(), data);'
    command.

    TL;DR: reset first, order matters, use 'index.getPageStore.writePage(getPos(), data);' to write
    */
    private final Data data;

    /**
     * Start of the data section in the page, that is the location of the end of the offset list
     */
    private int start;

    /**
     * The number of entries stored in this page.
     */
    private int entryCount;

    /**
     * Row offsets
     */
    private int[] offsets;

    /**
     * Index/Row data
     */
    private SearchRow[] rows;

    /**
     * Whether the page needs to be written to disk (dirty)
     */
    private Integer headerSize = null;

    private boolean written;
    private boolean dataWritten;


    /**
     * Use the Create method to create a new HashPage!!!
     * @param index  the index that this page is used by
     * @param pageId the page id
     * @param data   the data
     */
    private PagePersistentHash(PagePersistentHashIndex index, int pageId, Data data) {
        this.index = index;
        this.pageId = pageId;
        this.data = data;
        // Store the PageID (for caching)
        setPos(pageId);

        // yes i know; its the default value anyway.
        this.written = false;
        this.dataWritten = false;
    }

    /**
     * Create a new Page for the Persistent Hash index
     *
     * Allocate the new page before creating! Use the returned pageId
     *
     * @param index the index
     * @param pageId the id of the page to store in
     * @param pageType the type of the page to create. Either a BUCKET or a BUCKET_OVERFLOW type
     * @return
     */
    public static PagePersistentHash create(PagePersistentHashIndex index, int pageId, int pageType) {
        PagePersistentHash page = new PagePersistentHash(index, pageId, index.getPageStore().createData());
        // not sure what this line does exactly; im guessing this is for the 'undo' functionality
        index.getPageStore().logUndo(page, null);

        page.pageType = pageType;

        // initialize rows to be initially an empty array
        page.rows = SearchRow.EMPTY_ARRAY;

        // pre write header
        page.preWriteHead();

        // mark the offset at which the data section begins (after the header info)
        page.start = page.data.length();

        return page;
    }

    /**
     * Read a page from disk
     * @return
     */
    public static Page read(PagePersistentHashIndex index, Data data, int pageId) {
        PagePersistentHash page = new PagePersistentHash(index, pageId, data);
        page.read();
        return page;
    }

    /**
     * Read the page from disk
     */
    private void read() {
        data.reset();
        // page type
        this.pageType = data.readByte();
        // checksum
        data.readShortInt();
        // overflow page
        this.overflowPageId = data.readInt();
        // index id
        int indexId = data.readVarInt();
        if (indexId != this.index.getId()) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                        "got:" + indexId);
        }
        // next page id
        this.nextPageId = data.readInt();
        // entry count
        this.entryCount = data.readShortInt();
        this.offsets = new int[entryCount];
        this.rows = new SearchRow[entryCount];
        for (int i = 0; i < entryCount; i++) {
            this.offsets[i] = data.readShortInt();
        }
        this.start = data.length();

        readAllRows();

        // mark as written (no updates)
        written = true;
        dataWritten = true;
    }

    /**
     * Ensure all rows are read in memory.
     */
    private void readAllRows() {
        for (int i = 0; i < entryCount; i++) {
            SearchRow row = rows[i];
            if (row == null) {
                row = index.readRow(data, offsets[i]);
                rows[i] = row;
            }
        }
    }


    /**
     * Pre-Write the Header (add the header to the Data obj, but no change to disk occurs)
     *
     * Note: IDK if we really need to separate out the writing of the header and the rest of the page,
     *       I'm just following from what I saw in the pageBtreeLeaf...
     */
    private void preWriteHead() {
        data.reset();

        data.writeByte((byte)this.pageType);
        data.writeShortInt(0);
        data.writeInt(this.overflowPageId);
        data.writeVarInt(this.index.getId());
        data.writeInt(this.nextPageId);
        data.writeShortInt((short)this.entryCount);

        this.headerSize = data.length();
    }

    /**
     * Pre-Write the Data (add to Data obj, but no changes to disk)
     */
    private void preWriteData() {
        // No change, no write
        if (written)
            return;

        preWriteHead();

        for (int i = 0; i < entryCount; i++) {
            data.writeShortInt(this.offsets[i]);
        }

        if (!dataWritten) {
            for (int i = 0; i < entryCount; i++) {
                index.writeRow(this.data, this.offsets[i], this.rows[i]);
            }
            dataWritten = true;
        }

        written = true;
    }

    /**
     * Write the Data obj to disk
     */
    @Override
    public void write() {
        preWriteData();
        index.getPageStore().writePage(getPos(), this.data);
    }

    /**
     * Add the row to this Page's data.
     * If the Page is full, allocate a new overflow Page.
     *
     * This method should always succeed.
     * @return the amount of memory that was used to add the row
     */
    int addRow(SearchRow row) {
        // get how large each row is in this index -- maybe make this static since it probably doesnt change?
        // Check that nothing is of variable length before doing that
        int rowLength = index.getRowSize(row);

        // check that enough room for record + the offset entry
        if (rowLength + OFFSET_LENGTH > this.getAvailableSpace()) {
            // have the overflow page insert the record, return space that was used
            return this.getOverflowPage(true).addRow(row);

        } else {
            // this page has changed b/c new row added to the page
            written = false;
            dataWritten = false;
            changeCount = index.getPageStore().getChangeCount();

            this.start += OFFSET_LENGTH;
            // find offset at which this row will start
            int pageSize = index.getPageStore().getPageSize();
            int rowOffset = ( (entryCount == 0) ? pageSize : this.offsets[entryCount - 1] ) - rowLength;

            // insert data into arrays (store in order added)
            // TODO: keep sorted for faster lookup
            // Note for whoever implements the sorted insert, use the Page.add() to update the offsets being shifted over
            this.offsets = Page.insert(this.offsets, entryCount, entryCount, rowOffset);
            this.rows = Page.insert(this.rows, entryCount, entryCount, row);

            this.entryCount++;
            // update this page (not sure if this commits it to disk??, or triggers the write??)
            // this also might just be for the cache so that the page is kept in the cache???
            index.getPageStore().logUndo(this, null);
            index.getPageStore().update(this);

            // return space that was used by this entry
            return rowLength + OFFSET_LENGTH;
        }
    }

    /**
     * Find an entry in the Page
     *
     * Unlike the PageBTree, a HashIndex does not support ranges, thus we should only store exact matches into
     * the cursor.
     */
    void find(PagePersistentHashCursor cursor, SearchRow target) {
        // iterate through this page looking for exact matches
        for (int i = 0; i < this.entryCount; i++) {
            // If the row is an exact match to the target, add to cursor
            if (index.compareRows(this.rows[i], target) == 0) {
                cursor.addPosition(this.rows[i].getKey());
            }
        }
        // now iterate through the overflow
        PagePersistentHash overflowPage = this.getOverflowPage(false);
        if (overflowPage != null) {
            overflowPage.find(cursor, target);
        }
    }

    /**
     * Does the row removal logic.
     *
     *
     * External callers should call remove(), not removeRow()
     *
     * @param i the index of the row to be removed
     * @return the length of the row that was removed
     */
    private int removeRow(int i) {
        // get the location of the start of the next row
        int startNext = i > 0 ? this.offsets[i - 1] : index.getPageStore().getPageSize();
        // get size of row
        int rowLength = startNext - this.offsets[i];

        // decrement values as necessary
        this.start -= OFFSET_LENGTH;
        this.entryCount--;

        // remove the page from the array
        this.offsets = Page.remove(this.offsets, entryCount + 1, i);
        // update the offsets
        Page.add(this.offsets, i, entryCount, rowLength);
        // remove the rows
        this.rows = Page.remove(this.rows, entryCount + 1, i);

        return rowLength;
    }


    /**
     * Removes the requested row from the index.
     *
     * @param deletionTarget the row to remove
     * @return the amount of space that was freed from the removal
     */
    int remove(SearchRow deletionTarget) {
        // find the row that is being deleted
        for (int i = 0; i < this.entryCount; i++) {
            if (deletionTarget.getKey() == this.rows[i].getKey()) {
                index.getPageStore().logUndo(this, data);
                // this page has changed b/c it will remove the row
                written = false;
                dataWritten = false;
                changeCount = index.getPageStore().getChangeCount();

                return removeRow(i) + OFFSET_LENGTH;
            }
        }

        // was not in this page; now iterate through the overflow.
        PagePersistentHash overflowPage = this.getOverflowPage(false);
        if (overflowPage != null) {
            return overflowPage.remove(deletionTarget);
        } else {
            // reached end of overflow pages, still not found
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                    index.getSQL(new StringBuilder(), false).append(": ").append(deletionTarget).toString());
        }
    }

    /**
     * Does a quick compact pass, merging two blocks if they fit in one another.
     */
    public void lightCompact() {
        PagePersistentHash parentPage = this;
        PagePersistentHash childPage = this.getOverflowPage(false);

        while (childPage != null) {
            // if the parent is able to fit all the tuples stored in the child
            if (parentPage.getAvailableSpace() > childPage.getOccupiedSpace()) {

                parentPage.written = false;
                parentPage.dataWritten = false;
                parentPage.changeCount = index.getPageStore().getChangeCount();

                // loop through all the rows in the child and add them to the parent
                for (int i = 0; i < childPage.entryCount; i++) {
                    SearchRow row = childPage.rows[i];
                    parentPage.addRow(row);
                }

                // free the child

                // get the grandchild page if it exists
                PagePersistentHash grandchildPage = childPage.getOverflowPage(false);

                // if the overflow page has an overflow, make that this page's new overflow page
                if (grandchildPage != null) {
                    parentPage.overflowPageId = grandchildPage.pageId;
                } else {
                    parentPage.overflowPageId = NO_PAGE;
                }

                childPage.free(false);
                childPage = grandchildPage;
            } else {
                // iterate down the list
                parentPage = childPage;
                childPage = childPage.getOverflowPage(false);
            }
        }
    }

    /**
     * Calculates how much memory space is available in the page.
     *
     * @return int number of bytes
     */
    int getAvailableSpace() {
        return ((this.entryCount > 0) ? this.offsets[this.entryCount - 1 ] : index.getPageStore().getPageSize() ) - start;
    }

    /**
     * Calculates how much memory is available for tuples in a block (basically page size - header size)
     *
     * @return int number of bytes
     */
    int getTotalTupleSpace() {
        if(this.headerSize == null) {
            this.preWriteHead();
        }
        return index.getPageStore().getPageSize() - this.headerSize;
    }


    /**
     * Calculates how much memory space is being used by row entries into the page
     *
     * total page size - last offset + number of entries * 2 bytes each
     *
     * @return int number of bytes
     */
    int getOccupiedSpace() {
        return index.getPageStore().getPageSize() - ((this.entryCount > 0) ? this.offsets[this.entryCount - 1 ] : index.getPageStore().getPageSize()) + OFFSET_LENGTH * this.entryCount;
    }

    /**
     * Calculates how much memory the last row is occupying.
     * This includes the size of the offset entry.
     *
     * This is used to determine whether a page can be compacted by assessing whether
     * the row can be transferred to the parent page.
     *
     * Returns 0 if the page is empty.
     *
     * @return int number of bytes
     */
    private int getSizeOfLastRow() {
        if (this.entryCount <= 0) {
            return 0;
        } else if (this.entryCount == 1) {
            return OFFSET_LENGTH + (index.getPageStore().getPageSize() - this.offsets[entryCount - 1]);
        } else {
            return OFFSET_LENGTH + (this.offsets[entryCount - 2] - this.offsets[entryCount - 1]);
        }
    }

    /**
     * Get all the rows stored in this bucket and its overflow pages.
     * @param rows
     */
    void getAllRows(List<SearchRow> rows) {
        for (int i = 0; i < this.entryCount; i++) {
            rows.add(this.rows[i]);
        }
        PagePersistentHash overflowPage = this.getOverflowPage(false);
        if (overflowPage != null) {
            overflowPage.getAllRows(rows);
        }
    }

    /**
     * Reset this page by removing all entries
     *
     * The reset is "lazy"; it does not clear out all the data in the arrays. This also means that the array is non-empty
     * and will likely not need to reallocate when items are re-inserted (ideal when rehash).
     *
     * This also means that there may be unused references that will linger, causing some data to not be cleared by the GC
     * right away.
     */
    void reset() {
        // free the overflow page
        PagePersistentHash overflowPage = this.getOverflowPage(false);
        if (overflowPage != null) {
            overflowPage.free(true);
        }

        this.overflowPageId = NO_PAGE;

        this.start -= (this.entryCount * OFFSET_LENGTH);
        this.entryCount = 0;
        this.data.reset();

        this.written = false;
        this.dataWritten = false;
        changeCount = index.getPageStore().getChangeCount();
    }

    @Override
    public boolean canMove() {
        return super.canMove();
    }

    @Override
    public void moveTo(Session session, int newPos) {

    }

    /**
     * Returns the next page id, if there is one.
     * @return the page
     */
    int nextPage() {
        return nextPageId;
    }

    /**
     * Count the number of rows - only the number of rows in this bucket and its overflow bucket
     * @return the count
     */
    int rowCount() {
        int count = 0;
        count += this.entryCount;
        PagePersistentHash overflowPage = this.getOverflowPage(false);
        if (overflowPage != null) {
            count += overflowPage.rowCount();
        }
        return count;
    }

    /**
     * Set the ID of the next page that will appear in the page chain.
     *
     * @param pageId the page id
     */
    void setNextPageId(int pageId) {
        this.written = false;
        changeCount = index.getPageStore().getChangeCount();
        this.nextPageId = pageId;
    }

    /**
     * Frees this page
     *
     * @param freeRecursively if true, also frees all of its subpages (overflow pages)
     */
    void free(boolean freeRecursively) {
        PagePersistentHash overflowPage = this.getOverflowPage(false);
        if (freeRecursively && overflowPage != null) {
            overflowPage.free(true);
        }

        index.getPageStore().logUndo(this, data);
        index.getPageStore().free(this.pageId);
    }

    /**
     * Get the overflow page if there is one.
     *
     * @param createIfNoneExist toggles creation of the page if one does not already exist. If true, will never return null. If false, may be null if not present
     *
     * @return the overflow page, or null if there isnt one
     */
    private PagePersistentHash getOverflowPage(boolean createIfNoneExist) {
        // check to see if an overflow page has been created
        if (this.overflowPageId == NO_PAGE) {
            // check if it should create the missing overflow page
            if (createIfNoneExist) {
                // allocate a new page
                this.written = false;
                this.overflowPageId = this.index.getPageStore().allocatePage();
                changeCount = this.index.getPageStore().getChangeCount();
            } else {
                // creation not enabled; return null
                return null;
            }
        }

        // an overflow page has been allocated, get it from the cache/disk
        Page p = this.index.getPageStore().getPage(this.overflowPageId);

        // p is null if page was allocated, but not committed/saved (no changes to this page are required)
        if (p == null) {
            PagePersistentHash page = PagePersistentHash.create(this.index, this.overflowPageId, Page.TYPE_PERSISTENT_HASH_OVERFLOW_BUCKET);

            this.index.getPageStore().logUndo(page, null);
            this.index.getPageStore().update(page);
            return page;
        } else if (!(p instanceof PagePersistentHash)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, String.valueOf(p));
        }
        return (PagePersistentHash)p;
    }

    /**
     * Check if the object can be removed from the cache.
     * For example pinned objects can not be removed.
     *
     * @return true if it can be removed
     */
    @Override
    public boolean canRemove() {
        // copied from the other code, not sure how this actually functions
        return changeCount < index.getPageStore().getChangeCount();
    }

    /**
     * Get the estimated used memory.
     *
     * @return number of words (one word is 4 bytes)
     */
    @Override
    public int getMemory() {
        return index.getPageStore().getPageSize() - ((this.entryCount > 0) ? this.offsets[this.entryCount - 1 ] : index.getPageStore().getPageSize()) + start;
    }
}

