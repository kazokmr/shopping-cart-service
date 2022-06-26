package shopping.cart;

import akka.projection.eventsourced.EventEnvelope;
import akka.projection.jdbc.javadsl.JdbcHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.repository.HibernateJdbcSession;
import shopping.cart.repository.ItemPopularityRepository;

public class ItemPopularityProjectionHandler extends JdbcHandler<EventEnvelope<ShoppingCart.Event>, HibernateJdbcSession> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String tag;
    private final ItemPopularityRepository repo;

    public ItemPopularityProjectionHandler(String tag, ItemPopularityRepository repo) {
        this.tag = tag;
        this.repo = repo;
    }

    private ItemPopularity findOrNew(String itemId) {
        return repo.findById(itemId).orElseGet(() -> new ItemPopularity(itemId, 0L, 0L));
    }

    @Override
    public void process(HibernateJdbcSession session, EventEnvelope<ShoppingCart.Event> eventEventEnvelope) throws Exception, Exception {
        ShoppingCart.Event event = eventEventEnvelope.event();

        if (event instanceof ShoppingCart.ItemAdded) {
            ShoppingCart.ItemAdded added = (ShoppingCart.ItemAdded) event;
            String itemId = added.itemId;

            ItemPopularity existingItemPop = findOrNew(itemId);
            ItemPopularity updateItemPop = existingItemPop.changeCount(added.quantity);
            repo.save(updateItemPop);

            logger.info("ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
                    this.tag,
                    itemId,
                    updateItemPop.getCount());
        }
    }
}
