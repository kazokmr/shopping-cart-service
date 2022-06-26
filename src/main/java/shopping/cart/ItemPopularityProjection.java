package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.persistence.jdbc.query.javadsl.JdbcReadJournal;
import akka.persistence.query.Offset;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.ExactlyOnceProjection;
import akka.projection.javadsl.SourceProvider;
import akka.projection.jdbc.javadsl.JdbcProjection;
import org.springframework.orm.jpa.JpaTransactionManager;
import shopping.cart.repository.HibernateJdbcSession;
import shopping.cart.repository.ItemPopularityRepository;

import java.util.Optional;

public final class ItemPopularityProjection {

    private ItemPopularityProjection() {
    }

    // Projectionを初期化する
    public static void init(
            ActorSystem<?> system,
            JpaTransactionManager transactionManager,
            ItemPopularityRepository repository
    ) {
        // ShardedDaemonProcessを呼び出してProjectionを初期化する
        ShardedDaemonProcess.get(system)
                .init(
                        ProjectionBehavior.Command.class,   // messageはCommand
                        "ItemPopularityProjection",         // ProjectionのName
                        ShoppingCart.TAGS.size(),           // Projectionインスタンスの数
                        index -> ProjectionBehavior.create(
                                createProjectionFor(system, transactionManager, repository, index)), // Projectionの振る舞いを定義
                        ShardedDaemonProcessSettings.create(system),
                        Optional.of(ProjectionBehavior.stopMessage())
                );

    }

    private static ExactlyOnceProjection<Offset, EventEnvelope<ShoppingCart.Event>> createProjectionFor(
            ActorSystem<?> system,
            JpaTransactionManager transactionManager,
            ItemPopularityRepository repository,
            int index) {
        String tag = ShoppingCart.TAGS.get(index);      // インスタンスのIndexからTagを取得する

        SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
                EventSourcedProvider.eventsByTag(
                        system,
                        JdbcReadJournal.Identifier(),
                        tag
                );

        // Projectionでハンドリングする処理は、exactly-once(必ず１回)戦略で実行する
        return JdbcProjection.exactlyOnce(
                ProjectionId.of("ItemPopularityProjection", tag),
                sourceProvider,
                () -> new HibernateJdbcSession(transactionManager),
                () -> new ItemPopularityProjectionHandler(tag, repository),
                system
        );
    }
}
