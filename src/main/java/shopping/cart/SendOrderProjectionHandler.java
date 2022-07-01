package shopping.cart;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.javadsl.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.order.proto.Item;
import shopping.order.proto.OrderRequest;
import shopping.order.proto.ShoppingOrderService;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static akka.Done.done;

public final class SendOrderProjectionHandler extends Handler<EventEnvelope<ShoppingCart.Event>> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ClusterSharding sharding;
    private final Duration timeout;
    private final ShoppingOrderService orderService;

    public SendOrderProjectionHandler(ActorSystem<?> system, ShoppingOrderService orderService) {
        sharding = ClusterSharding.get(system);
        timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout");
        this.orderService = orderService;
    }

    @Override
    public CompletionStage<Done> process(EventEnvelope<ShoppingCart.Event> eventEventEnvelope) throws Exception {
        if (eventEventEnvelope.event() instanceof ShoppingCart.CheckedOut) {
            ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) eventEventEnvelope.event();
            return sendOrder(checkedOut);
        } else {
            return CompletableFuture.completedFuture(done());
        }
    }

    private CompletionStage<Done> sendOrder(ShoppingCart.CheckedOut checkedOut) {
        // CartIdから対象のEntityを取り出しCart内の商品リストを取得する
        EntityRef<ShoppingCart.Command> entityRef =
                sharding.entityRefFor(ShoppingCart.ENTITY_KEY, checkedOut.cartId);
        CompletionStage<ShoppingCart.Summary> reply =
                entityRef.ask(ShoppingCart.Get::new, timeout);
        // Cart内の商品リストをRequestメッセージに渡してOrderServiceに注文する
        return reply.thenCompose(
                cart -> {
                    List<Item> protoItems =
                            cart.items.entrySet().stream()
                                    .map(
                                            entry ->
                                                    Item.newBuilder()
                                                            .setItemId(entry.getKey())
                                                            .setQuantity(entry.getValue())
                                                            .build()
                                    )
                                    .collect(Collectors.toList());
                    log.info("Sending order of {} items for cart {}", cart.items.size(), checkedOut.cartId);
                    OrderRequest orderRequest =
                            OrderRequest.newBuilder().setCartId(checkedOut.cartId).addAllItems(protoItems).build();
                    return orderService.order(orderRequest).thenApply(response -> done());
                }
        );
    }
}
