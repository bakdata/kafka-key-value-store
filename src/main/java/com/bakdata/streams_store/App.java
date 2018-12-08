package com.bakdata.streams_store;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;


public class App {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();
        Properties props = new Properties();

        String topicName = null;
        String hostName = null;
        Integer port = null;
        String storeName = "key-value-store";

        try {
            Namespace res = parser.parseArgs(args);

            topicName = res.getString("topic");
            hostName = res.getString("hostname");
            port = res.getInt("port");
            String applicationId = res.getString("applicationId");
            List<String> streamsProps = res.getList("streamsConfig");
            String streamsConfig = res.getString("streamsConfigFile");

            if (streamsProps == null && streamsConfig == null) {
                throw new ArgumentParserException("Either --streams-props or --streams.config must be specified.", parser);
            }

            if (streamsConfig != null) {
                try (InputStream propStream = Files.newInputStream(Paths.get(streamsConfig))) {
                    props.load(propStream);
                }
            }

            if (streamsProps != null) {
                for (String prop : streamsProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.put(pieces[0], pieces[1]);
                }
            }

            props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostName + ":" + port);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }

        final StreamsBuilder builder = new StreamsBuilder();
        KeyValueBytesStoreSupplier stateStore = Stores.inMemoryKeyValueStore(storeName);

        KTable<String, String> table = builder.table(
            topicName,
            Materialized.<String, String>as(stateStore)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final RestService restService = new RestService(streams, storeName, hostName, port);
        restService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                restService.stop();
            } catch (Exception e) {}
        }));

        streams.start();
    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newFor("streams-processor").build()
                .defaultHelp(true)
                .description("This Kafka Streams application is used to interactively query values from Kafka topics");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("process messages from this topic");

        parser.addArgument("--streams-props")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("streamsConfig")
                .help("kafka streams related configuration properties like bootstrap.servers etc. " +
                        "These configs take precedence over those passed via --streams.config.");

        parser.addArgument("--streams.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("streamsConfigFile")
                .help("streams config properties file.");

        parser.addArgument("--application-id")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("APPLICATION-ID")
                .dest("applicationId")
                .setDefault("streams-processor-default-application-id")
                .help("The id of the streams application to use. Useful for monitoring and resetting the streams application state.");

        parser.addArgument("--hostname")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("HOSTNAME")
                .setDefault("localhost")
                .help("The host name of this machine / pod / container. Used for inter-processor communication.");

        parser.addArgument("--port")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("PORT")
                .setDefault(8080)
                .help("The TCP Port for the HTTP REST Service");

        return parser;
    }
}
