package org.h2.index;

import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.value.Value;

public class PagePersistentHashIndex extends PageIndex {

    private static final float OCCUPANCY_LIMIT = 0.75f;

    /**
     * The number of rows stored in this index
     */
    private long rowCount;

    /**
     * The number of pages used by this index
     */
    private long pageCount;

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
    private long[] pageIds;

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
    protected PagePersistentHashIndex(Table newTable, int id, String name, IndexColumn[] newIndexColumns, IndexType newIndexType) {
        super(newTable, id, name, newIndexColumns, newIndexType);
        // stuff
    }

    /**
     * Write back the row count if it has changed.
     *
     * TODO: keep track of total is required anyway for linear hashing. Just store that value
     */
    @Override
    public void writeRowCount() {

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

    }

    /**
     * Add the row to the index
     *
     * @param session the session to use
     * @param row the row to add
     */
    @Override
    public void add(Session session, Row row) {

    }

    /**
     * Remove a row from the index
     *
     * @param session the session
     * @param row the row
     */
    @Override
    public void remove(Session session, Row row) {

    }

    /**
     * Calculate the hash
     *
     * @param row the row being hashed
     * @return the hash value
     */
    private long calculateHashValue(SearchRow row) {
        long hashValue = 0L;
        for (Column c : columns) {
            int colId = c.getColumnId();
            hashValue += (colId * row.getValue(colId).hashCode());
        }
        return hashValue;
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

        long hashCode = calculateHashValue(first);

        long hashedBlock = hashCode & ~((~0L) << this.significantBits);

        // remove the leading bit if the block doesnt exist
        if (hashedBlock > this.pageCount) {
            hashedBlock = hashCode &  ~((~0L << (this.significantBits - 1)));
        }

        // get the page
        // find in the page

        return null;
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
        return 0;
    }

    /**
     * Remove the index.
     *
     * TODO: Free all of the allocated pages.
     *
     * @param session the session
     */
    @Override
    public void remove(Session session) {

    }

    @Override
    public void truncate(Session session) {

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
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    /**
     * Get the size of a row (only the part that is stored in the index).
     *
     * @param dummy a dummy data page to calculate the size
     * @param row the row
     * @return the number of bytes
     */
    int getRowSize(Data dummy, SearchRow row) {
        int rowsize = Data.getVarLongLen(row.getKey());
        for (Column col : columns) {
            Value v = row.getValue(col.getColumnId());
            rowsize += dummy.getValueLen(v);
        }
        return rowsize;
    }

    PageStore getPageStore() {
        return this.database.getPageStore();
    }
}
