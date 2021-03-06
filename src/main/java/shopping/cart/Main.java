package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.grpc.GrpcClientSettings;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.orm.jpa.JpaTransactionManager;
import shopping.cart.proto.ShoppingCartService;
import shopping.cart.repository.ItemPopularityRepository;
import shopping.cart.repository.SpringIntegration;
import shopping.order.proto.ShoppingOrderService;
import shopping.order.proto.ShoppingOrderServiceClient;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // Main Actorの振る舞いでActorSystemを起動する
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "ShoppingCartService");
        try {
            init(system);
        } catch (Exception e) {
            logger.error("Terminating due to initialization failure.", e);
            system.terminate();
        }
    }

    public static void init(ActorSystem<Void> system) {
        // Akka Clusterで利用するためのAkka Managementを初期化する
        AkkaManagement.get(system).start();
        ClusterBootstrap.get(system).start();

        ShoppingCart.init(system);

        // Projectionの初期化
        ApplicationContext springContext = SpringIntegration.applicationContext(system);
        ItemPopularityRepository itemPopularityRepository = springContext.getBean(ItemPopularityRepository.class);
        JpaTransactionManager transactionManager = springContext.getBean(JpaTransactionManager.class);

        ItemPopularityProjection.init(system, transactionManager, itemPopularityRepository);
        PublishEventsProjection.init(system, transactionManager);
        SendOrderProjection.init(system, transactionManager, orderServiceClient(system));

        // Configファイルから必要な情報をとり、gRPCサーバーを起動する
        Config config = system.settings().config();
        String grpcInterface = config.getString("shopping-cart-service.grpc.interface");
        int grpcPort = config.getInt("shopping-cart-service.grpc.port");
        ShoppingCartService grpcService = new ShoppingCartServiceImpl(system, itemPopularityRepository);
        ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService);
    }

    static ShoppingOrderService orderServiceClient(ActorSystem<?> system) {
        GrpcClientSettings orderServiceClientSettings =
                GrpcClientSettings.connectToServiceAt(
                                system.settings().config().getString("shopping-order-service.host"),
                                system.settings().config().getInt("shopping-order-service.port"),
                                system
                        )
                        .withTls(false);
        return ShoppingOrderServiceClient.create(orderServiceClientSettings, system);
    }
}
