package org.h2.index;

import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;

public class PagePersistentHashIndex extends PageIndex {

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

    @Override
    public void writeRowCount() {

    }

    @Override
    public void checkRename() {

    }

    @Override
    public void close(Session session) {

    }

    @Override
    public void add(Session session, Row row) {

    }

    @Override
    public void remove(Session session, Row row) {

    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return null;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {
        return 0;
    }

    @Override
    public void remove(Session session) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return null;
    }

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


    // package - level encapsulation
    PageStore getPageStore() {
        return this.database.getPageStore();
    }
}
