package dashboard;

import gossip.GossipManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ClusterApplication {

    private static final int GOSSIP_PORT = 8081; // Порт за Gossip (UDP)
    private static final int WEB_PORT = 8080;   // Порт за Dashboard (HTTP)

    public static void main(String[] args) {
        System.setProperty("server.port", String.valueOf(WEB_PORT));
        SpringApplication.run(ClusterApplication.class, args);
    }

    // Креирање на GossipManager Bean
    @Bean
    public GossipManager gossipManager() throws Exception {
        // Овој јазол (8081) се стартува без seed nodes.
        String[] seedNodes = {};
        return new GossipManager("127.0.0.1", GOSSIP_PORT, seedNodes);
    }
}