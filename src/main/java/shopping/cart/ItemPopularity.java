package shopping.cart;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

@Entity
@Table(name = "item_popularity")
public class ItemPopularity {
    @Id
    private final String itemId;

    @Version
    private final Long version;

    private final long count;

    public ItemPopularity() {
        this.version = null;
        this.itemId = "";
        this.count = 0;
    }

    public ItemPopularity(String itemId, long version, long count) {
        this.itemId = itemId;
        this.version = version;
        this.count = count;
    }

    public String getItemId() {
        return itemId;
    }

    public Long getVersion() {
        return version;
    }

    public long getCount() {
        return count;
    }

    public ItemPopularity changeCount(long delta) {
        return new ItemPopularity(itemId, version, count + delta);
    }

}
