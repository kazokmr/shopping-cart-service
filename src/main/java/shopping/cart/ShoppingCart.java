package shopping.cart;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandlerWithReply;
import akka.persistence.typed.javadsl.CommandHandlerWithReplyBuilder;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventHandlerBuilder;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies;
import akka.persistence.typed.javadsl.ReplyEffect;
import akka.persistence.typed.javadsl.RetentionCriteria;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public final class ShoppingCart extends EventSourcedBehaviorWithEnforcedReplies<ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State> {

    static final EntityTypeKey<Command> ENTITY_KEY = EntityTypeKey.create(Command.class, "ShoppingCart");

    private final String cartId;

    // Akkaクラスタのクラスターシャーディングを用いて各クラスタノードにカートエンティティを生成させる
    public static void init(ActorSystem<?> system) {
        ClusterSharding.get(system).init(Entity.of(ENTITY_KEY, entityContext -> ShoppingCart.create(entityContext.getEntityId())));
    }

    // ShoppingCartオブジェクトを生成する
    public static Behavior<Command> create(String cartId) {
        return Behaviors.setup(ctx -> EventSourcedBehavior.start(new ShoppingCart(cartId), ctx));
    }

    // snapshotによってCartEntityを最新の状態に復旧させる
    @Override
    public RetentionCriteria retentionCriteria() {
        return RetentionCriteria.snapshotEvery(100, 3);
    }

    // ShoppingCartのコンストラクタ。復旧に失敗した場合は状態をリセットするように設定する
    private ShoppingCart(String cartId) {
        super(PersistenceId.of(ENTITY_KEY.name(), cartId), SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
        this.cartId = cartId;
    }

    @Override
    public State emptyState() {
        return new State();
    }

    // Shopping Cartアクターをサポートする全てのコマンドのインタフェース
    interface Command extends CborSerializable {
    }

    // カートに商品を追加するコマンド。StatusReply<Summary> はコマンドによってイベントが発行されて正常に永続化されたときのレスポンスになる
    public static final class AddItem implements Command {
        final String itemId;
        final int quantity;
        final ActorRef<StatusReply<Summary>> replayTo;

        public AddItem(String itemId, int quantity, ActorRef<StatusReply<Summary>> replyTo) {
            this.itemId = itemId;
            this.quantity = quantity;
            this.replayTo = replyTo;
        }
    }

    // ショッピングカートの状態の概要. 応答メッセージとして利用する
    public static final class Summary implements CborSerializable {
        final Map<String, Integer> items;

        @JsonCreator
        public Summary(Map<String, Integer> items) {
            this.items = new HashMap<>(items);
        }
    }

    abstract static class Event implements CborSerializable {
        public final String cartId;

        public Event(String cartId) {
            this.cartId = cartId;
        }
    }

    static final class ItemAdded extends Event {
        public final String itemId;
        public final int quantity;

        public ItemAdded(String cartId, String itemId, int quantity) {
            super(cartId);
            this.itemId = itemId;
            this.quantity = quantity;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            ItemAdded other = (ItemAdded) obj;

            if (quantity != other.quantity) return false;
            if (!cartId.equals(other.cartId)) return false;
            return itemId.equals(other.itemId);
        }

        @Override
        public int hashCode() {
            int result = cartId.hashCode();
            result = 31 * result + itemId.hashCode();
            result = 31 * result + quantity;
            return result;
        }
    }

    static final class State implements CborSerializable {
        final Map<String, Integer> items;

        public State() {
            this(new HashMap<>());
        }

        public State(Map<String, Integer> items) {
            this.items = items;
        }

        public boolean hasItem(String itemId) {
            return items.containsKey(itemId);
        }

        public State updateItem(String itemId, int quantity) {
            if (quantity == 0) {
                items.remove(itemId);
            } else {
                items.put(itemId, quantity);
            }
            return this;
        }

        public Summary toSummary() {
            return new Summary(items);
        }

        public int itemCount(String itemId) {
            return items.get(itemId);
        }

        public boolean isEmpty() {
            return items.isEmpty();
        }
    }

    @Override
    public CommandHandlerWithReply<Command, Event, State> commandHandler() {
        CommandHandlerWithReplyBuilder<Command, Event, State> builder = new CommandHandlerWithReplyBuilder<>();
        builder.forAnyState().onCommand(AddItem.class, this::onAddItem);
        return builder.build();
    }

    private ReplyEffect<Event, State> onAddItem(State state, AddItem cmd) {
        if (state.hasItem(cmd.itemId)) {
            return Effect().reply(cmd.replayTo, StatusReply.error("Item '" + cmd.itemId + "' was already added to this shopping cart"));
        } else if (cmd.quantity <= 0) {
            return Effect().reply(cmd.replayTo, StatusReply.error("Quantity must be greater than zero"));
        } else {
            return Effect().persist(new ItemAdded(cartId, cmd.itemId, cmd.quantity)).thenReply(cmd.replayTo, updatedCart -> StatusReply.success(updatedCart.toSummary()));
        }
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
        EventHandlerBuilder<State,Event> builder = new EventHandlerBuilder<>();
        builder.forAnyState().onEvent(ItemAdded.class, (state, event) -> state.updateItem(event.itemId, event.quantity));
        return builder.build();
//        return newEventHandlerBuilder().forAnyState().onEvent(ItemAdded.class, (state, evt) -> state.updateItem(evt.itemId, evt.quantity)).build();
    }
}

