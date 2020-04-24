package kafka.streams.interactive.query.controllers;

import kafka.streams.interactive.query.InventoryServiceInteractiveQueries;
import kafka.streams.interactive.query.ProductBean;
import kafka.streams.interactive.query.ProductPurchaseCountBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.mddarr.inventory.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;


@RestController
public class InventoryController {
    static final String PLAY_EVENTS = "play-events";
    static final String SONG_FEED = "song-feed";
    static final String TOP_FIVE_KEY = "all";
    static final String TOP_FIVE_SONGS_STORE = "top-five-songs";
    static final String ALL_SONGS = "all-songs";

    @Autowired
    private InteractiveQueryService interactiveQueryService;

    private final Log logger = LogFactory.getLog(getClass());

    @RequestMapping("/product/idx")
    public ProductBean product(@RequestParam(value="id") Long id) {
        final ReadOnlyKeyValueStore<Long, Product> productStore =
                interactiveQueryService.getQueryableStore(ALL_SONGS, QueryableStoreTypes.<Long, Product>keyValueStore());

        final Product product = productStore.get(id);
        if (product == null) {
            throw new IllegalArgumentException("hi");
        }
        return new ProductBean(product.getBrand(), product.getName()) ;
    }

    @RequestMapping("/charts/top-five")
    @SuppressWarnings("unchecked")
    public List<ProductPurchaseCountBean> topFive(@RequestParam(value="genre") String genre) {

        HostInfo hostInfo = interactiveQueryService.getHostInfo(TOP_FIVE_SONGS_STORE,
                TOP_FIVE_KEY, new StringSerializer());

        if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
            logger.info("Top Five songs request served from same host: " + hostInfo);
            return topFiveSongs(TOP_FIVE_KEY, TOP_FIVE_SONGS_STORE);
        }
        else {
            //find the store from the proper instance.
            logger.info("Top Five songs request served from different host: " + hostInfo);
            RestTemplate restTemplate = new RestTemplate();
            return restTemplate.postForObject(
                    String.format("http://%s:%d/%s", hostInfo.host(),
                            hostInfo.port(), "charts/top-five?genre=Punk"), "punk", List.class);
        }
    }

    private List<ProductPurchaseCountBean> topFiveSongs(final String key, final String storeName) {
        final ReadOnlyKeyValueStore<String, InventoryServiceInteractiveQueries.TopFiveProducts> topFiveStore =
                interactiveQueryService.getQueryableStore(storeName, QueryableStoreTypes.<String, InventoryServiceInteractiveQueries.TopFiveProducts>keyValueStore());

        // Get the value from the store
        final InventoryServiceInteractiveQueries.TopFiveProducts value = topFiveStore.get(key);
        if (value == null) {
            throw new IllegalArgumentException(String.format("Unable to find value in %s for key %s", storeName, key));
        }
        final List<ProductPurchaseCountBean> results = new ArrayList<>();
        value.forEach(productPurchaseCount -> {

            HostInfo hostInfo = interactiveQueryService.getHostInfo(ALL_SONGS,
                    productPurchaseCount.getProductId(), new LongSerializer());

            if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
                logger.info("Song info request served from same host: " + hostInfo);

                final ReadOnlyKeyValueStore<Long, Product> productStore =
                        interactiveQueryService.getQueryableStore(ALL_SONGS, QueryableStoreTypes.<Long, Product>keyValueStore());

                final Product product = productStore.get(productPurchaseCount.getProductId());
                results.add(new ProductPurchaseCountBean(product.getBrand(),product.getName(), productPurchaseCount.getCount()));
            }
            else {
                logger.info("Song info request served from different host: " + hostInfo);
                RestTemplate restTemplate = new RestTemplate();
                ProductBean product = restTemplate.postForObject(
                        String.format("http://%s:%d/%s", hostInfo.host(),
                                hostInfo.port(), "song/idx?id=" + productPurchaseCount.getProductId()),  "id", ProductBean.class);
                results.add(new ProductPurchaseCountBean(product.getBrand(),product.getName(),productPurchaseCount.getCount()));
            }


        });
        return results;
    }
}