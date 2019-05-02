package org.h2.index;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import java.util.ArrayList;
import org.h2.table.PageStoreTable;


/**
 * This class is for the Cursor implementation of a HashIndex.
 *
 * According to the Cursor description: "For indexes without the ability
 * to iterate (e.g. a hash index) only one row is returned."
 *
 */
public class PagePersistentHashCursor implements Cursor {

    private final Session session;
    private final ArrayList<Long> positions;
    private final PageStoreTable tableData;

    private int index = -1;

    PagePersistentHashCursor(Session session, PageStoreTable tableData) {
        this.session = session;
        this.tableData = tableData;
        this.positions = new ArrayList<>();
    }

    void addPosition(long position) {
        this.positions.add(position);
    }

    @Override
    public Row get() {
        if (index < 0 || index > positions.size()) {
            return null;
        }
        return tableData.getRow(session, positions.get(index));
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        return positions != null && ++index < positions.size();
    }

    @Override
    public boolean previous() {
        return positions != null && --index >= 0;
    }
}
