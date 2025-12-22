package dashboard;

import gossip.GossipManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

// main spring boot aplikacija sto go startuva web dashboard-ot i eden gossip node
@SpringBootApplication
public class ClusterApplication {

    // udp port na koj ovoj process ke go slusa gossip-soobrakjajot
    private static final int GOSSIP_PORT = 8081;

    // http port na koj e dostupen dashboardot (rest endpointi i static html)
    private static final int WEB_PORT = 8080;

    public static void main(String[] args) {
        // setiranje na web portot za spring boot serverot
        System.setProperty("server.port", String.valueOf(WEB_PORT));
        SpringApplication.run(ClusterApplication.class, args);
    }

    // kreira eden singleton GossipManager bean sto ke se koristi i od rest kontrolerot i od ui-to
    // ovoj process e isto swim/gossip node so adresata 127.0.0.1:GOSSIP_PORT bez seed nodes
    @Bean
    public GossipManager gossipManager() throws Exception {
        String host = "127.0.0.1";

        // dashboard nodeot ne se povrzuva na nikoj seed, drugite nodes go koristat nego kako seed
        String[] seedNodes = {};

        return new GossipManager(host, GOSSIP_PORT, seedNodes);
    }
}
