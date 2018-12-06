package com.bakdata.streams_store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Path("messages")
public class RestService {

    private final KafkaStreams streams;
    private final String storeName;
    private HostInfo hostInfo;
    private Server jettyServer;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();

    public RestService(final KafkaStreams streams, final String storeName, final String hostName, final int port) {
        this.streams = streams;
        this.storeName = storeName;
        this.hostInfo = new HostInfo(hostName, port);
    }

    public void start() throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostInfo.port());
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    @GET
    @Path("/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public KeyValueBean valueByKey(@PathParam("key") final String key, @Context UriInfo uriInfo) {

        final StreamsMetadata metadata = streams.metadataForKey(storeName, key, Serdes.String().serializer());
        if (metadata == null) {
            throw new NotFoundException();
        }

         if (!metadata.hostInfo().equals(hostInfo)) {
             return fetchValue(metadata.hostInfo(), uriInfo.getPath(), new GenericType<KeyValueBean>() {});
         }

        final ReadOnlyKeyValueStore<String, String> store = streams.store(storeName, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        final String value = store.get(key);
        if (value == null) {
            throw new NotFoundException();
        }

        return new KeyValueBean(key, value);
    }

    private <T> T fetchValue(final HostInfo host, final String path, GenericType<T> responseType) {
        return client.target(String.format("http://%s:%d/%s", host.host(), host.port(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(responseType);
    }

    @AllArgsConstructor @NoArgsConstructor
    @Getter @Setter
    public class ProcessorMetadata {
        private String host;
        private int port;
        private List<Integer> topicPartitions;
    }

    @GET()
    @Path("/processors")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ProcessorMetadata> processors() {
        return streams.allMetadataForStore(storeName)
                .stream()
                .map(metadata -> new ProcessorMetadata(
                        metadata.host(),
                        metadata.port(),
                        metadata.topicPartitions().stream()
                                .map(TopicPartition::partition)
                                .collect(Collectors.toList()))
                )
                .collect(Collectors.toList());
    }
}

