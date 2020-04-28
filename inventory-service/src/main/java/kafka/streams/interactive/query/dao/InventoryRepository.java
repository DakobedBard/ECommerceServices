package kafka.streams.interactive.query.dao;

import kafka.streams.interactive.query.entity.ProductEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface InventoryRepository extends CrudRepository<ProductEntity, String> {

}
