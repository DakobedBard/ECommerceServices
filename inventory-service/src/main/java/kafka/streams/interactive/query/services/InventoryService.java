package kafka.streams.interactive.query.services;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.mddarr.products.Product;
import org.mddarr.products.PurchaseCount;
import org.mddarr.products.PurchaseEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.function.BiConsumer;

@Service
public class InventoryService {

    public static final String PURCHASE_EVENTS = "purchase-events";
    public static final String PRODUCT_FEED = "product-feed";
    public static final String TOP_FIVE_KEY = "all";
    public static final String TOP_FIVE_PRODUCTS_STORE = "top-five-products";
    public static final String ALL_PRODUCTS = "all-products";
    private static final String PRODUCT_PLAY_COUNT_STORE = "product-play-count";

    static final String TOP_FIVE_SONGS_BY_GENRE_STORE = "top-five-products-by-brand";

    //
    @Bean
    public BiConsumer<KStream<String, PurchaseEvent>, KTable<String, Product>> process() {

        return (s, t) -> {
            // create and configure the SpecificAvroSerdes required in this example
            final Map<String, String> serdeConfig = Collections.singletonMap(
                    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

            final SpecificAvroSerde<PurchaseEvent> purchaseEventSerde = new SpecificAvroSerde<>();
            purchaseEventSerde.configure(serdeConfig, false);

            final SpecificAvroSerde<Product> keyProductSerde = new SpecificAvroSerde<>();
            keyProductSerde.configure(serdeConfig, true);

            final SpecificAvroSerde<Product> valueProductSerde = new SpecificAvroSerde<>();
            valueProductSerde.configure(serdeConfig, false);

            final SpecificAvroSerde<PurchaseCount> productPurchaseCountSerde = new SpecificAvroSerde<>();
            productPurchaseCountSerde.configure(serdeConfig, false);

            // Accept play events that have a duration >= the minimum
            final KStream<String, PurchaseEvent> purchasesByProductId =
                    s.map((key, value) -> KeyValue.pair(value.getProductId(), value));
//								filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
//								// repartition based on song id
//								.map((key, value) -> KeyValue.pair(value.getSongId(), value));

            // join the plays with song as we will use it later for charting
            final KStream<String, Product> productPurchases = purchasesByProductId.leftJoin(t,
                    (value1, song) -> song,
                    Joined.with(Serdes.String(), purchaseEventSerde, valueProductSerde));

            // create a state store to track song play counts
            final KTable<Product, Long> productPurchaseCounts = productPurchases.groupBy((songId, song) -> song,
                    Grouped.with(keyProductSerde, valueProductSerde))
                    .count(Materialized.<Product, Long, KeyValueStore<Bytes, byte[]>>as(PRODUCT_PLAY_COUNT_STORE)
                            .withKeySerde(valueProductSerde)
                            .withValueSerde(Serdes.Long()));

            final TopFiveSerde topFiveSerde = new TopFiveSerde();

            // Compute the top five charts for each genre. The results of this computation will continuously update the state
            // store "top-five-songs-by-genre", and this state store can then be queried interactively via a REST API (cf.
            // MusicPlaysRestService) for the latest charts per genre.
            productPurchaseCounts.groupBy((product, purchase_count) ->
								KeyValue.pair(product.getBrand().toLowerCase(),
										new PurchaseCount(product.getProductId(), purchase_count)),
						Grouped.with(Serdes.String(), productPurchaseCountSerde))
						// aggregate into a TopFiveSongs instance that will keep track
						// of the current top five for each genre. The data will be available in the
						// top-five-songs-genre store
						.aggregate(TopFiveProducts::new,
								(aggKey, value, aggregate) -> {
									aggregate.add(value);
									return aggregate;
								},
								(aggKey, value, aggregate) -> {
									aggregate.remove(value);
									return aggregate;
								},
								Materialized.<String, TopFiveProducts, KeyValueStore<Bytes, byte[]>>as(TOP_FIVE_SONGS_BY_GENRE_STORE)
										.withKeySerde(Serdes.String())
										.withValueSerde(topFiveSerde)
                        );
//
//				// Compute the top five chart. The results of this computation will continuously update the state
//				// store "top-five-songs", and this state store can then be queried interactively via a REST API (cf.
//				// MusicPlaysRestService) for the latest charts per genre.
            productPurchaseCounts.groupBy((product, purchase_count) ->
								KeyValue.pair(TOP_FIVE_KEY,
										new PurchaseCount(product.getProductId(), purchase_count)),
						Grouped.with(Serdes.String(), productPurchaseCountSerde))
						.aggregate(TopFiveProducts::new,
								(aggKey, value, aggregate) -> {
									aggregate.add(value);
									return aggregate;
								},
								(aggKey, value, aggregate) -> {
									aggregate.remove(value);
									return aggregate;
								},
								Materialized.<String, TopFiveProducts, KeyValueStore<Bytes, byte[]>>as(TOP_FIVE_PRODUCTS_STORE)
										.withKeySerde(Serdes.String())
										.withValueSerde(topFiveSerde)
						);
        };

    }


    public static class TopFiveProducts implements Iterable<PurchaseCount> {
        private final Map<String, PurchaseCount> currentProducts = new HashMap<>();
        private final TreeSet<PurchaseCount> topFive = new TreeSet<>((o1, o2) -> {
            final int result = o2.getCount().compareTo(o1.getCount());
            if (result != 0) {
                return result;
            }
            return o1.getProductId().compareTo(o2.getProductId());
        });

        public void add(final PurchaseCount productPurchaseCount) {
            if(currentProducts.containsKey(productPurchaseCount.getProductId())) {
                topFive.remove(currentProducts.remove(productPurchaseCount.getProductId()));
            }
            topFive.add(productPurchaseCount);
            currentProducts.put(productPurchaseCount.getProductId(), productPurchaseCount);
            if (topFive.size() > 5) {
                final PurchaseCount last = topFive.last();
                currentProducts.remove(last.getProductId());
                topFive.remove(last);
            }
        }

        void remove(final PurchaseCount value) {
            topFive.remove(value);
            currentProducts.remove(value.getProductId());
        }

        @Override
        public Iterator<PurchaseCount> iterator() {
            return topFive.iterator();
        }
//	}
    }

    private static class TopFiveSerde implements Serde<TopFiveProducts> {

        @Override
        public Serializer<TopFiveProducts> serializer() {

            return new Serializer<TopFiveProducts>() {
                @Override
                public void configure(final Map<String, ?> map, final boolean b) {
                }
                @Override
                public byte[] serialize(final String s, final TopFiveProducts topFiveProducts) {

                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    final DataOutputStream
                            dataOutputStream =
                            new DataOutputStream(out);
                    try {
                        for (PurchaseCount purchaseCount : topFiveProducts) {
                            dataOutputStream.writeUTF(purchaseCount.getProductId());
                            dataOutputStream.writeLong(purchaseCount.getCount());
                        }
                        dataOutputStream.flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return out.toByteArray();
                }
            };
        }

        @Override
        public Deserializer<TopFiveProducts> deserializer() {

            return (s, bytes) -> {
                if (bytes == null || bytes.length == 0) {
                    return null;
                }
                final TopFiveProducts result = new TopFiveProducts();

                final DataInputStream
                        dataInputStream =
                        new DataInputStream(new ByteArrayInputStream(bytes));

                try {
                    while(dataInputStream.available() > 0) {
                        result.add(new PurchaseCount(dataInputStream.readUTF(),
                                dataInputStream.readLong()));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return result;
            };
        }
    }


}
