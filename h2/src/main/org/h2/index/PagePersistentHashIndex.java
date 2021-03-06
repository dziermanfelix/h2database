package org.h2.index;

import org.h2.api.ErrorCode;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.*;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.value.Value;
import java.util.ArrayList;

public class PagePersistentHashIndex extends PageIndex {

    private static final float DATA_OCCUPANCY_LIMIT = 0.75f;

    /**
     * The number of rows stored in this index
     */
    private long rowCount;

    /**
     * The number of pages used by this index
     */
    private int pageCount;

    /**
     * The amount of memory available for storing data
     * This only counts the buckets; not the overflow buckets
     */
    private long dataMemoryAvailable;

    /**
     * The amount of memory used by the rows
     */
    private long dataMemoryUsed;

    /**
     * A dummy data object used to calculate how much space is consumed.
     */
    private static Data dummy;

    /**
     * The number of bits that are significant
     *
     * Calculate this by:
     * ceil((log PageCount) / (log 2))
     */
    private byte significantBits;

    /**
     * The page Ids.
     *
     * Since we risk having many many pages in memory, it should be best
     * if we dont have a direct reference to the pages, and instead use the
     * cache to request the Page object when necessary (?)
     *
     * I dont know how the caching system works really well so maybe not...
     */
    private ArrayList<Integer> pageIds;

    /**
     * The page store table
     */
    private PageStoreTable storeTable;

    /**
     * The PageStore
     */
    private PageStore store;

    /**
     * Initialize the page store index.
     *
     * @param newTable the table
     * @param id the object id
     * @param name the index name
     * @param newIndexColumns the columns that are indexed or null if this is
     *            not yet known
     * @param newIndexType the index type
     */
    protected PagePersistentHashIndex(PageStoreTable newTable, int id, String name, IndexColumn[] newIndexColumns, IndexType newIndexType) {
        super(newTable, id, name, newIndexColumns, newIndexType);

        this.storeTable = newTable;
        this.store = this.database.getPageStore();

        PagePersistentHashIndex.dummy = this.store.createData();

        // stuff
        // TODO: create a single page (the root/initial page) -- if new index
        // TODO: load the data (from somewhere) -- if not new index
        // TODO: set the initial values
        // TODO: get occupancy data;
        /*
        Occupancy data: get the memory used by each row inserted (add to size when adding row, remove when removing)
         */
    }

    /**
     * Write back the row count if it has changed.
     *
     * TODO: keep track of total is required anyway for linear hashing. Just store that value
     */
    @Override
    public void writeRowCount() {
        // do a write?
    }

    /**
     * Check if this object can be renamed. System objects may not be renamed.
     *
     * TODO: pretty sure we can just do nothing here.
     */
    @Override
    public void checkRename() {

    }

    /**
     * Close this index.
     *
     * TODO: unclear what this does/mean, this might be done when the db closes or something to give the index a chance to save volatile data
     *
     * @param session the session used to write data
     */
    @Override
    public void close(Session session) {
        // do write?
    }

    /**
     * Add the row to the index
     *
     * @param session the session to use
     * @param row the row to add
     */
    @Override
    public void add(Session session, Row row) {
        // hash the row & get the page
        int hashCode = this.calculateHashValue(row);
        PagePersistentHash page = this.getPageFromHash(hashCode);

        SearchRow newRow = getSearchRow(row);

        try {
            // add the row
            this.dataMemoryUsed += page.addRow(newRow);
            this.rowCount++;

            doPageReallocation();
        } finally {
            store.incrementChangeCount();
        }
    }

    /**
     * Does the check to see if a new page is needed, and if so, does the reallocation of the pages
     * by comparing & moving the rows to their new destination pages
     */
    private void doPageReallocation() {
        if (this.dataMemoryUsed / (float)this.dataMemoryAvailable > PagePersistentHashIndex.DATA_OCCUPANCY_LIMIT) {
            // allocate a new bucket page
            int newPageId = this.store.allocatePage();
            PagePersistentHash newPage = PagePersistentHash.create(this, newPageId, Page.TYPE_PERSISTENT_HASH_BUCKET);
            this.pageIds.add(newPageId);
            this.store.logUndo(newPage, null);

            // register the new memory that is made available
            this.dataMemoryAvailable += newPage.getAvailableSpace();

            // add the new page & find how many bits are significant
            this.pageCount++;
            this.significantBits = (byte)Math.ceil((Math.log(this.pageCount)) / (Math.log(2)));

            // now get the items at the lower page and move items that belong to this page over.
            int lowerPageHashValue = (this.pageCount - 1) & ~(~0 << (this.significantBits - 1));

            PagePersistentHash lowerPage = getPage(this.pageIds.get(lowerPageHashValue));

            ArrayList<SearchRow> rows = new ArrayList<>();
            // get all the rows at the lower page
            lowerPage.getAllRows(rows);
            // reset the lower page to be "empty".
            lowerPage.reset();

            // rehash each entry to their respective block
            for (SearchRow r : rows) {
                if (this.calculateHashValue(r) == lowerPageHashValue) {
                    lowerPage.addRow(r);
                } else {
                    newPage.addRow(r);
                }
            }

            this.store.update(newPage);
            this.store.update(lowerPage);
        }
    }

    /**
     * Remove a row from the index
     *
     * @param session the session
     * @param row the row
     */
    @Override
    public void remove(Session session, Row row) {
        // hash the row & get the page
        int hashCode = this.calculateHashValue(row);
        PagePersistentHash page = this.getPageFromHash(hashCode);

        SearchRow deletedRow = getSearchRow(row);
        try {
            // remove the row
            this.dataMemoryUsed -= page.remove(deletedRow);
            this.rowCount--;
        } finally {
            store.incrementChangeCount();
        }
    }

    /**
     * Calculate the hash
     *
     * @param row the row being hashed
     * @return the hash value
     */
    private int calculateHashValue(SearchRow row) {
        int hashValue = 0;
        for (Column c : columns) {
            int colId = c.getColumnId();
            hashValue += (colId * row.getValue(colId).hashCode());
        }
        return hashValue;
    }

    /**
     * Get the Page that holds the specified hash value
     *
     * @param hash the hash code
     * @return the corresponding PagePersistentHash page
     */
    private PagePersistentHash getPageFromHash(int hash) {
        int bitmask = ~((~0) << this.significantBits);

        int hashedBlock = hash & bitmask;

        // remove the leading bit if the block doesnt exist
        if (hashedBlock > this.pageCount) {
            hashedBlock = hash & (bitmask >>> 1);
        }

        return getPage(this.pageIds.get(hashedBlock));
    }

    /**
     * Find a row or a list of rows and create a cursor to iterate over the
     * result.
     *
     * Since this is a Hash, the first and the last must be equivalent.
     * Cannot be null since this hash does not support ranges.
     *
     * @param session the session
     * @param first the first row, or null for no limit
     * @param last the last row, or null for no limit
     * @return the cursor to iterate over the results
     */
    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        // If either are null; throw an exception
        if (first == null || last == null) {
            throw DbException.throwInternalError("Got: " + first + " " + last + "; but expected no null values");
        }
        // If the first and the last are not the same, throw an exception
        if (first != last) {
            if (compareKeys(first, last) != 0) {
                throw DbException.throwInternalError("Hash expects first and last to be equivalent");
            }
        }

        // hash the row & get the page
        int hashCode = this.calculateHashValue(first);
        PagePersistentHash page = this.getPageFromHash(hashCode);

        // create empty cursor
        PagePersistentHashCursor cursor = new PagePersistentHashCursor(session, this.storeTable);

        // find the data
        page.find(cursor, first);

        return cursor;
    }

    /**
     * Estimate the cost to search for rows given the search mask.
     * There is one element per column in the search mask.
     * For possible search masks, see IndexCondition.
     *
     * TODO: unsure how much calculation there is to do here, I suppose we can just insert a random number since it should be constant time anyway.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param allColumnsSet the set of all columns
     * @return
     */
    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {
        // this looks like: if any of the fields aren't straight equality, then return MAX_VALUE (aka cost of going thru full db)
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        // return cost. idk what this value actually means, im going to leave it at 10.
        return 10;
    }

    private void removeAllRows() {
        for (Integer id : this.pageIds) {
            PagePersistentHash page = getPage(id);
            page.free(true);
        }
        this.pageIds.clear();

        this.rowCount = 0;
        this.pageCount = 0;
        this.dataMemoryAvailable = 0;
        this.dataMemoryUsed = 0;
        this.significantBits = 0;

        store.incrementChangeCount();
    }

    /**
     * Remove the index.
     *
     * Frees all the pages associated with this index.
     *
     * @param session the session
     */
    @Override
    public void remove(Session session) {
        removeAllRows();
    }



    @Override
    public void truncate(Session session) {
        removeAllRows();

        if (this.storeTable.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        this.storeTable.setRowCount(0);
    }

    @Override
    public boolean canScan() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    /**
     * Hash can't do this. Throw unsupported exception
     *
     * @param session the session
     * @param first true if the first (lowest for ascending indexes) or last
     *            value should be returned
     * @return
     */
    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("Persistent Hash");
    }

    /**
     * Unclear what this means.
     * @return
     */
    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return this.rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return this.rowCount;
    }

    /**
     * Get the estimated amount of disk space used by this index.
     * We will ignore overflow blocks for this and return the number of buckets * size of the buckets
     * @return estimated disk space in bytes
     */
    @Override
    public long getDiskSpaceUsed() {
        return this.dataMemoryAvailable;
    }

    /**
     * Get the size of a row (only the part that is stored in the index).
     *
     * @param row the row
     * @return the number of bytes
     */
    int getRowSize(SearchRow row) {
        int rowSize = Data.getVarLongLen(row.getKey());
        for (Column col : columns) {
            Value v = row.getValue(col.getColumnId());
            rowSize += PagePersistentHashIndex.dummy.getValueLen(v);
        }
        return rowSize;
    }

    PageStore getPageStore() {
        return this.store;
    }

    /**
     * Get the page from the store
     * If the page was not in the store, then a new one is created.
     *
     * @param id the page id
     * @return the page.
     */
    PagePersistentHash getPage(int id) {
        Page p = store.getPage(id);
        if (p == null) {
            PagePersistentHash page = PagePersistentHash.create(this, id, Page.TYPE_PERSISTENT_HASH_BUCKET);

            store.logUndo(page, null);
            store.update(page);
        } else if (!(p instanceof PagePersistentHash)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, String.valueOf(p));
        }
        return (PagePersistentHash)p;
    }


    /**
     * Create a search row for this row.
     *
     * @param row the row
     * @return the search row
     */
    private SearchRow getSearchRow(Row row) {
        SearchRow r = table.getTemplateSimpleRow(columns.length == 1);
        r.setKey(row);
        for (Column c : columns) {
            int idx = c.getColumnId();
            r.setValue(idx, row.getValue(idx));
        }
        return r;
    }

}
