package shopping.cart;

import akka.Done;
import akka.kafka.javadsl.SendProducer;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.javadsl.Handler;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

public class PublishEventsProjectionHandler extends Handler<EventEnvelope<ShoppingCart.Event>> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String topic;
    private final SendProducer<String, byte[]> sendProducer; // Akka(Alpakka)のKafka用コネクタ

    public PublishEventsProjectionHandler(String topic, SendProducer<String, byte[]> sendProducer) {
        this.topic = topic;
        this.sendProducer = sendProducer;
    }

    @Override
    public CompletionStage<Done> process(EventEnvelope<ShoppingCart.Event> eventEventEnvelope) throws Exception {

        ShoppingCart.Event event = eventEventEnvelope.event();

        // cartIdをKeyとして分割機能(DefaultPartitioner)を利用する
        // これによって同じCartのEventを同じPartitionで実行させる
        String key = event.cartId;

        // eventをProtobufでシリアライズし、指定のTopicへ送信するデータを作成する
        ProducerRecord<String, byte[]> producerRecord =
                new ProducerRecord<>(topic, key, serialize(event));

        return sendProducer
                .send(producerRecord)
                .thenApply(
                        recordMetadata -> {
                            logger.info(
                                    "published event [{}] to topic/partition {}/{}",
                                    event,
                                    topic,
                                    recordMetadata.partition()
                            );
                            return Done.done();
                        }
                );
    }

    private static byte[] serialize(ShoppingCart.Event event) {
        final ByteString protoMessage;  // protobufの対象メッセージ
        final String fullName;          // 対象メッセージの識別子

        if (event instanceof ShoppingCart.ItemAdded) {
            ShoppingCart.ItemAdded itemAdded = (ShoppingCart.ItemAdded) event;
            protoMessage =
                    shopping.cart.proto.ItemAdded.newBuilder()
                            .setCartId(itemAdded.cartId)
                            .setItemId(itemAdded.itemId)
                            .setQuantity(itemAdded.quantity)
                            .build()
                            .toByteString();
            fullName = shopping.cart.proto.ItemAdded.getDescriptor().getFullName();

        } else if (event instanceof ShoppingCart.CheckedOut) {
            ShoppingCart.CheckedOut checkedOut = (ShoppingCart.CheckedOut) event;
            protoMessage =
                    shopping.cart.proto.CheckedOut.newBuilder()
                            .setCartId(checkedOut.cartId)
                            .build()
                            .toByteString();
            fullName = shopping.cart.proto.CheckedOut.getDescriptor().getFullName();

        } else {
            throw new IllegalArgumentException("Unknown event type: " + event.getClass());
        }

        // Anyでパッケージすることで型情報を持たせることができるのでデシリアライズの際に便利になる
        return Any.newBuilder()
                .setValue(protoMessage)
                .setTypeUrl("shopping-cart-service/" + fullName)
                .build()
                .toByteArray();
    }
}
