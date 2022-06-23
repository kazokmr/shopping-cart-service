package shopping.cart;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shopping.cart.proto.ShoppingCartService;

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

        // Configファイルから必要な情報をとり、gRPCサーバーを起動する
        Config config = system.settings().config();
        String grpcInterface = config.getString("shopping-cart-service.grpc.interface");
        int grpcPort = config.getInt("shopping-cart-service.grpc.port");
        ShoppingCartService grpcService = new ShoppingCartServiceImpl();
        ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService);
    }
}
