package shopping.cart.repository;

import org.springframework.data.repository.Repository;
import shopping.cart.ItemPopularity;

import java.util.Optional;

public interface ItemPopularityRepository extends Repository<ItemPopularity, String> {
    ItemPopularity save(ItemPopularity itemPopularity);

    Optional<ItemPopularity> findById(String id);
}
