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
import akka.persistence.typed.javadsl.CommandHandlerWithReplyBuilderByState;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;
import akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies;
import akka.persistence.typed.javadsl.ReplyEffect;
import akka.persistence.typed.javadsl.RetentionCriteria;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class ShoppingCart extends EventSourcedBehaviorWithEnforcedReplies<ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State> {

    static final EntityTypeKey<Command> ENTITY_KEY = EntityTypeKey.create(Command.class, "ShoppingCart");

    // Projectionインタンスに付けるTagのリスト
    static final List<String> TAGS = List.of("carts-0", "carts-1", "carts-2", "carts-3", "carts-4");

    // Akkaクラスタのクラスターシャーディングを用いて各クラスタノードにカートエンティティを生成させる
    public static void init(ActorSystem<?> system) {
        ClusterSharding.get(system)
                .init(
                        Entity.of(
                                ENTITY_KEY,
                                // Entityのハッシュ値からどのTagに割り当てるかを決める。同じEntityは同じProjectionインスタンスで処理させたいため.
                                entityContext -> {
                                    int i = Math.abs(entityContext.getEntityId().hashCode() % TAGS.size());
                                    String selectedTag = TAGS.get(i);
                                    return ShoppingCart.create(entityContext.getEntityId(), selectedTag);
                                }));
    }

    // ShoppingCartオブジェクトを生成する
    public static Behavior<Command> create(String cartId, String projectionTag) {
        return Behaviors.setup(ctx -> EventSourcedBehavior.start(new ShoppingCart(cartId, projectionTag), ctx));
    }

    private final String cartId;

    private final String projectionTag;

    // ShoppingCartのコンストラクタ。復旧に失敗した場合は状態をリセットするように設定する
    private ShoppingCart(String cartId, String projectionTag) {
        super(
                PersistenceId.of(ENTITY_KEY.name(), cartId),
                SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1));
        this.cartId = cartId;
        this.projectionTag = projectionTag;
    }

    // snapshotによってCartEntityを最新の状態に復旧させる
    @Override
    public RetentionCriteria retentionCriteria() {
        return RetentionCriteria.snapshotEvery(100, 3);
    }

    // 状態をリセットする
    @Override
    public State emptyState() {
        return new State();
    }

    // projectionTagをTagに割り当てる
    @Override
    public Set<String> tagsFor(Event event) {
        return Collections.singleton(projectionTag);
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

    // カートの商品を購入するコマンド
    public static final class Checkout implements Command {
        final ActorRef<StatusReply<Summary>> replyTo;

        @JsonCreator
        public Checkout(ActorRef<StatusReply<Summary>> replyTo) {
            this.replyTo = replyTo;
        }
    }

    // 現在のCartの状態を取得するコマンド。イベントを呼ばずに状態だけを返す
    public static final class Get implements Command {
        final ActorRef<Summary> replyTo;

        @JsonCreator
        public Get(ActorRef<Summary> replyTo) {
            this.replyTo = replyTo;
        }
    }


    // ショッピングカートの状態の概要. 応答メッセージとして利用する
    public static final class Summary implements CborSerializable {
        final Map<String, Integer> items;
        final boolean checkedOut;

        @JsonCreator
        public Summary(Map<String, Integer> items, boolean checkedOut) {
            this.items = new HashMap<>(items);
            this.checkedOut = checkedOut;
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

    static final class CheckedOut extends Event {

        final Instant eventTime;

        public CheckedOut(String cartId, Instant eventTime) {
            super(cartId);
            this.eventTime = eventTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CheckedOut that = (CheckedOut) o;
            return Objects.equals(eventTime, that.eventTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventTime);
        }
    }

    static final class State implements CborSerializable {
        final Map<String, Integer> items;
        private Optional<Instant> checkoutDate;

        public State() {
            this(new HashMap<>(), Optional.empty());
        }

        public State(Map<String, Integer> items, Optional<Instant> checkoutDate) {
            this.items = items;
            this.checkoutDate = checkoutDate;
        }

        public boolean isCheckedOut() {
            return checkoutDate.isPresent();
        }

        public State checkout(Instant now) {
            checkoutDate = Optional.of(now);
            return this;
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
            return new Summary(items, isCheckedOut());
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
        return openShoppingCart()
                .orElse(checkedOutShoppingCart())
                .orElse(getCommandHandler())
                .build();
    }

    // Checkoutがまだならコマンドを受け付ける
    private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> openShoppingCart() {
        return newCommandHandlerWithReplyBuilder()
                .forState(state -> !state.isCheckedOut())
                .onCommand(AddItem.class, this::onAddItem)
                .onCommand(Checkout.class, this::onCheckout);
    }

    // Checkout済なら全てのコマンドを受け付けない
    private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> checkedOutShoppingCart() {
        return newCommandHandlerWithReplyBuilder()
                .forState(State::isCheckedOut)
                .onCommand(
                        AddItem.class,
                        cmd -> Effect().reply(
                                cmd.replayTo,
                                StatusReply.error("Can't add an item to an already checked out shopping cart")
                        )
                )
                .onCommand(Checkout.class,
                        cmd -> Effect().reply(
                                cmd.replyTo,
                                StatusReply.error("Can't checkout already checked out shopping cart")
                        )
                );
    }

    private CommandHandlerWithReplyBuilderByState<Command, Event, State, State> getCommandHandler() {
        return newCommandHandlerWithReplyBuilder()
                .forAnyState()
                .onCommand(Get.class, (state, cmd) -> Effect().reply(cmd.replyTo, state.toSummary()));
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

    private ReplyEffect<Event, State> onCheckout(State state, Checkout cmd) {
        if (state.isEmpty()) {
            return Effect().reply(cmd.replyTo, StatusReply.error("Cannot checkout an empty shopping cart"));
        }
        return Effect().persist(new CheckedOut(cartId, Instant.now())).thenReply(cmd.replyTo, updateCart -> StatusReply.success(updateCart.toSummary()));
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
        return newEventHandlerBuilder()
                .forAnyState()
                .onEvent(ItemAdded.class, (state, evt) -> state.updateItem(evt.itemId, evt.quantity))
                .onEvent(CheckedOut.class, (state, evt) -> state.checkout(evt.eventTime))
                .build();
    }
}

