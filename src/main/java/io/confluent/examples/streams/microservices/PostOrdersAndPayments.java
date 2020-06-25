package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Payment;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.Paths;
import io.confluent.examples.streams.utils.MonitoringInterceptorUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import java.time.Duration;
import java.util.*;

import static io.confluent.examples.streams.microservices.domain.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.buildPropertiesFromConfigFile;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class PostOrdersAndPayments {

    private static GenericType<OrderBean> newBean() {
        return new GenericType<OrderBean>() {
        };
    }

    private static void sendPayment(final String id,
                                    final Payment payment,
                                    final String bootstrapServers,
                                    final String schemaRegistryUrl,
                                    final Properties defaultConfig) {

        //System.out.printf("-----> id: %s, payment: %s%n", id, payment);

        final SpecificAvroSerializer<Payment> paymentSerializer = new SpecificAvroSerializer<>();
        final boolean isKeySerde = false;

        paymentSerializer.configure(
            Schemas.buildSchemaRegistryConfigMap(defaultConfig),
            isKeySerde);

        final Properties producerConfig = new Properties();
        producerConfig.putAll(defaultConfig);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "payment-generator");

        final KafkaProducer<String, Payment> paymentProducer =
                new KafkaProducer<>(producerConfig, new StringSerializer(), paymentSerializer);

        final ProducerRecord<String, Payment> record = new ProducerRecord<>("payments", id, payment);
        paymentProducer.send(record);
        paymentProducer.close();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws Exception {

        final int NUM_CUSTOMERS = 6;
        final List<Product> productTypeList = Arrays.asList(Product.JUMPERS, Product.UNDERPANTS, Product.STOCKINGS);
        final Random randomGenerator = new Random();

        final Options opts = new Options();
        opts.addOption(Option.builder("b")
                    .longOpt("bootstrap-server").hasArg().desc("Kafka cluster bootstrap server string").build())
                .addOption(Option.builder("s")
                    .longOpt("schema-registry").hasArg().desc("Schema Registry URL").build())
                .addOption(Option.builder("o")
                    .longOpt("order-service-url").hasArg().desc("Order Service URL").build())
                .addOption(Option.builder("c")
                    .longOpt("config-file").hasArg().desc("Java properties file with configurations for Kafka Clients").build())
               .addOption(Option.builder("n")
                    .longOpt("order-id").hasArg().desc("The starting order id for posting new orders").build())
                .addOption(Option.builder("h")
                    .longOpt("help").hasArg(false).desc("Show usage information").build());

        final CommandLine cl = new DefaultParser().parse(opts, args);
        if (cl.hasOption("h")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Post Orders and Payments", opts);
            return;
        }

        final String bootstrapServers = cl.getOptionValue("bootstrap-server", "localhost:9092");
        final String schemaRegistryUrl = cl.getOptionValue("schema-registry", "http://localhost:8081");
        final String orderServiceUrl = cl.getOptionValue("order-service-url", "http://localhost:5432");
        final Optional<String> configFile = Optional.ofNullable(cl.getOptionValue("config-file", null));
        final String stateDir = cl.getOptionValue("state-dir", "/tmp/kafka-streams");
        final int startingOrderId = Integer.parseInt(cl.getOptionValue("order-id", "1"));

        final Properties defaultConfig =
                buildPropertiesFromConfigFile(Optional.ofNullable(cl.getOptionValue("config-file", null)));

        Schemas.configureSerdes(defaultConfig);

        OrderBean returnedOrder;
        final Paths path = new Paths(orderServiceUrl);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonFeature.class);
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 60000)
                    .property(ClientProperties.READ_TIMEOUT, 60000);
        final Client client = ClientBuilder.newClient(clientConfig);

        // send one order every 1 second
        int i = startingOrderId;
        while (true) {

            final int randomCustomerId = randomGenerator.nextInt(NUM_CUSTOMERS);
            final Product randomProduct = productTypeList.get(randomGenerator.nextInt(productTypeList.size()));

            final OrderBean inputOrder = new OrderBean(
                    id(i),
                    randomCustomerId,
                    OrderState.CREATED,
                    randomProduct,
                    1,
                    1d);

            // POST order to OrdersService
            System.out.printf("Posting order to: %s   .... ", path.urlPost());
            final Response response = client.target(path.urlPost())
                                            .request(APPLICATION_JSON_TYPE)
                                            .post(Entity.json(inputOrder));
            System.out.printf("Response: %s %n", response.getStatus());

            // GET the bean back explicitly
            System.out.printf("Getting order from: %s   .... ", path.urlGet(i));
            returnedOrder = client.target(path.urlGet(i))
                                  .queryParam("timeout", Duration.ofMinutes(1).toMillis() / 2)
                                  .request(APPLICATION_JSON_TYPE)
                                  .get(newBean());

            if (!inputOrder.equals(returnedOrder)) {
                System.out.printf("Posted order %d does not equal returned order: %s%n", i, returnedOrder.toString());
            } else {
                System.out.printf("Posted order %d equals returned order: %s%n", i, returnedOrder.toString());
            }

            // Send payment
            final Payment payment = new Payment("Payment:1234", id(i), "CZK", 1000.00d);
            sendPayment(payment.getId(), payment, bootstrapServers, schemaRegistryUrl, defaultConfig);

            Thread.sleep(5000L);
            i++;
        }
    }

}
