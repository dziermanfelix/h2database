/**
 *
 * Maxence Weyrich
 *
 */

package org.h2.index;

import org.h2.result.SearchRow;
import org.h2.engine.Session;
import org.h2.store.Data;
import org.h2.store.Page;

/**
 * A linear hash page that contains hash index data. Format:
 * <ul>
 * <li>page type: byte</li>
 * <li>checksum: short</li>
 * <li>index id: varInt</li>
 * <li>key offset: byte</li>
 * <li>key value: varlong</li>
 * <li>list of offsets: short</li>
 * <li>data (key: varLong, value,...)</li>
 * </ul>
 */
public class PagePersistentHash extends Page {

    /**
     * The index.
     */
    private PagePersistentHashIndex index;

    /**
     * The page ID
     */
    private int pageId;

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
     * Start of the data section in the page
     */
    private int start;


    /**
     * The number of entries stored in this page.
     */
    private int entryCount;

    /**
     * The row keys of the entries.
     */
    private long[] keys;

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
    private boolean written;

    /**
     * Estimated memory usage
     */
    // TODO: compute
    private static final int memoryEstimated = 1024;


    /**
     * Use the Create method to create a new HashPage!!!
     * @param index  the index that this page is used by
     * @param pageId the page id
     * @param data   the data page(?)
     */
    private PagePersistentHash(PagePersistentHashIndex index, int pageId, Data data) {
        this.index = index;
        this.pageId = pageId;
        this.data = data;
        // Store the PageID (for caching)
        setPos(pageId);

        // yes i know; its the default value anway.
        this.written = false;
    }

    /**
     * Create a new Page for the Persistent Hash index
     *
     * Allocate the new page before creating! Use the returned pageId
     *
     * @param index the index
     * @param pageId the id of the page to store in
     * @return
     */
    public static PagePersistentHash create(PagePersistentHashIndex index, int pageId) {
        PagePersistentHash page = new PagePersistentHash(index, pageId, index.getPageStore().createData());
        // not sure what this line does exactly; im guessing this is for the 'undo' functionality
        index.getPageStore().logUndo(page, null);

        // initialize rows to be initially an empty array
        page.rows = SearchRow.EMPTY_ARRAY;

        // pre write header
        page.preWriteHead();

        // mark the offset at which the data section begins (after the header info)
        page.start = page.data.length();

        return page;
    }

    /**
     * Read a
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
        // do reads

        // mark as written (no updates)
        written = true;
    }


    /**
     * Pre-Write the Header (add the header to the Data obj, but no change to disk occurs)
     *
     * Note: IDK if we really need to separate out the writing of the header and the rest of the page,
     *       I'm just following from what I saw in the pageBtreeLeaf...
     */
    private void preWriteHead() {
        // data.reset();

        // TODO: Write the header using the data object, e.g.:
        // data.writeInt(this.index.getId());
    }

    /**
     * Pre-Write the Data (add to Data obj, but no changes to disk)
     */
    private void preWriteData() {
        // No change, no write
        if (written)
            return;

        preWriteHead();
        // iterate through data

        written = true;
    }

    /**
     * Write the Data obj to disk
     */
    @Override
    public void write() {
        preWriteData();
        index.getPageStore().writePage(getPos(), data);
    }

    @Override
    public boolean canMove() {
        return super.canMove();
    }

    @Override
    public void moveTo(Session session, int newPos) {

    }

    /**
     * Check if the object can be removed from the cache.
     * For example pinned objects can not be removed.
     *
     * @return true if it can be removed
     */
    @Override
    public boolean canRemove() {
        //TODO
        return false;
    }

    /**
     * Get the estimated used memory.
     *
     * @return number of words (one word is 4 bytes)
     */
    @Override
    public int getMemory() {
        return this.memoryEstimated;
    }
}

