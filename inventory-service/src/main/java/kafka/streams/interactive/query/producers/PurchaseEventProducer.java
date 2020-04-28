/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.streams.interactive.query.producers;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
//import kafka.streams.interactive.query.avro.PlayEvent;
import kafka.streams.interactive.query.services.InventoryService;
import org.mddarr.products.Product;
import org.mddarr.products.PurchaseEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class PurchaseEventProducer {

	public static void main(String... args) throws Exception {
		initProductsDB();
		populateTopics();
	}

	private static void initProductsDB() {
		final Map<String, String> serdeConfig = Collections.singletonMap(
				AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

		final SpecificAvroSerializer<Product> productSerializer = new SpecificAvroSerializer<>();
		productSerializer.configure(serdeConfig, false);

		Map<String, Object> props = new HashMap<>();
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, productSerializer.getClass());

		DefaultKafkaProducerFactory<String, Product> pf1 = new DefaultKafkaProducerFactory<>(props);

		KafkaTemplate<String, Product> template1 = new KafkaTemplate<>(pf1, true);
		template1.setDefaultTopic(InventoryService.PRODUCT_FEED);

		Statement stmt = null;
		Connection c = null;
		String row;
		String[] columns;
		UUID uuid;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager
					.getConnection("jdbc:postgresql://localhost:5433/productdb",
							"postgres", "postgres");
			BufferedReader br = new BufferedReader(new FileReader("stack/db/products.csv"));
			while((row = br .readLine()) != null) {
				uuid = UUID.randomUUID();
				columns = row.split(",");
				stmt = c.createStatement();
				String sql = String.format("INSERT INTO product_entity (\"id\",\"brand\",\"name\",\"price\") "
						+ "VALUES ('%s', '%s', '%s', %d );",uuid.toString(), columns[0], columns[1], Long.parseLong(columns[2]));
				stmt.executeUpdate(sql);
				template1.sendDefault(uuid.toString(),new Product(uuid.toString(),columns[0], columns[1], Long.parseLong(columns[2])));
				System.out.println(columns[0]);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}

	public static List<Product> getProductsFromDB(){
		List<Product> products = new ArrayList<>();
		try (Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5433/productdb",
				"postgres", "postgres");
			 PreparedStatement pst = con.prepareStatement("SELECT * FROM product_entity");
			 ResultSet rs = pst.executeQuery()) {
			while (rs.next()) {
				products.add(new Product (rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4)));
			}
		} catch (SQLException e) {
			System.err.println(e.getClass().getName()+": "+e.getMessage());
		}
		return products;
	}

	public static void populateTopics() throws Exception{

		final Map<String, String> serdeConfig = Collections.singletonMap(
				AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		// Set serializers and
		final SpecificAvroSerializer<PurchaseEvent> purchaseEventSerializer = new SpecificAvroSerializer<>();
		purchaseEventSerializer.configure(serdeConfig, false);
		final SpecificAvroSerializer<Product> productSerializer = new SpecificAvroSerializer<>();
		productSerializer.configure(serdeConfig, false);

		Map<String, Object> props = new HashMap<>();
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, purchaseEventSerializer.getClass());

		Map<String, Object> props1 = new HashMap<>(props);
		props1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, productSerializer.getClass());

		DefaultKafkaProducerFactory<String, Product> pf1 = new DefaultKafkaProducerFactory<>(props1);
		KafkaTemplate<String, Product> template1 = new KafkaTemplate<>(pf1, true);
		template1.setDefaultTopic(InventoryService.PRODUCT_FEED);

		List<Product> products = getProductsFromDB();

        products.forEach(product -> {
			System.out.println("Writing product information for '" + product.getName() + "' to input topic " +
					InventoryService.PRODUCT_FEED);
			template1.sendDefault(product.getProductId(), product);
		});

		DefaultKafkaProducerFactory<String, PurchaseEvent> pf = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<String, PurchaseEvent> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(InventoryService.PURCHASE_EVENTS);

		final long purchase_quantity = 3;
		final Random random = new Random();

		// send a play event every 100 milliseconds
		while (true) {
			final Product product = products.get(random.nextInt(products.size()));
			System.out.println("Writing purchase event for product " + product.getName() + " to input topic " +
					InventoryService.PURCHASE_EVENTS);
			template.sendDefault("uk", new PurchaseEvent(1L, product.getProductId(), purchase_quantity));

			Thread.sleep(100L);
		}

	}


}

//		final List<Product> products = Arrays.asList(new Product("a",
//						"Fresh Fruit For Rotting Vegetables",
//						"Dead Kennedys",
//                        (long) 15.4),
//				new Product("4aa8c996-ad90-4b87-860e-dd02c1ed8061",
//                        "Nike",
//                        "Jordan X10",
//                        (long) 100.4),
//				new Product("c",
//						"Addidas",
//						"All Star",
//						450L),
//				new Product("d",
//						"Puma",
//						"Shoes",
//						240L),
//				new Product("e",
//						"Addidas",
//						"Dumbo",
//						240L),
//				new Product("f",
//						"Addidas",
//						"Silly",
//						240L)
//
//
//		);
//
//		Map<String, Object> props = new HashMap<>();
//		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(ProducerConfig.RETRIES_CONFIG, 0);
//		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, purchaseEventSerializer.getClass());
//
//		Map<String, Object> props1 = new HashMap<>(props);
//		props1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		props1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, productSerializer.getClass());
//
//		DefaultKafkaProducerFactory<String, Product> pf1 = new DefaultKafkaProducerFactory<>(props1);
//		KafkaTemplate<String, Product> template1 = new KafkaTemplate<>(pf1, true);
//		template1.setDefaultTopic(InventoryService.PRODUCT_FEED);
//
//        products.forEach(product -> {
//			System.out.println("Writing product information for '" + product.getName() + "' to input topic " +
//					InventoryService.PRODUCT_FEED);
//			template1.sendDefault(product.getProductId(), product);
//		});
//
//		DefaultKafkaProducerFactory<String, PurchaseEvent> pf = new DefaultKafkaProducerFactory<>(props);
//		KafkaTemplate<String, PurchaseEvent> template = new KafkaTemplate<>(pf, true);
//		template.setDefaultTopic(InventoryService.PURCHASE_EVENTS);
//
//		final long purchase_quantity = 3;
//		final Random random = new Random();
//
//		// send a play event every 100 milliseconds
//		while (true) {
//			final Product product = products.get(random.nextInt(products.size()));
//			System.out.println("Writing purchase event for product " + product.getName() + " to input topic " +
//					InventoryService.PURCHASE_EVENTS);
//			template.sendDefault("uk", new PurchaseEvent(1L, product.getProductId(), purchase_quantity));
//
//			Thread.sleep(100L);
//		}
//	}
//}
