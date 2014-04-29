package poc.vertx;

import java.util.HashMap;
import java.util.HashSet;

import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

public class SimpleServer extends Verticle {

  public void start() {

     final HashSet<HttpClient> clients = new HashSet<>();
     clients.add(vertx.createHttpClient().setHost("10.249.51.2").setPort(80));
     clients.add(vertx.createHttpClient().setHost("10.249.51.3").setPort(80));
     final HashSet<HttpClient> clients2 = new HashSet<>();
     clients2.add(vertx.createHttpClient().setHost("10.249.51.4").setPort(80));
     clients2.add(vertx.createHttpClient().setHost("10.249.51.5").setPort(80));

     final HashMap<String, HashSet<HttpClient>> vhosts = new HashMap<>();
     vhosts.put("teste.qa02.globoi.com:8080", clients);
     vhosts.put("teste2.qa02.globoi.com:8080", clients2);

//    final HttpClient client = vertx.createHttpClient().setHost("127.0.0.1").setPort(8081);

      vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

        @Override
        public void handle(final HttpServerRequest sRequest) {
            sRequest.response().setChunked(true);

            String header = sRequest.headers().get("Host");
            final HttpClientRequest cReq = ((HttpClient) vhosts.get(header).toArray()[getChoice(clients.size())]).request(sRequest.method(), sRequest.uri(),
                    new Handler<HttpClientResponse>() {

                public void handle(HttpClientResponse cResponse) {

                    Pump.createPump(cResponse, sRequest.response()).start();

                    cResponse.endHandler(new VoidHandler() {
                        public void handle() {
                            sRequest.response().end();
                            vertx.setTimer(2000L, new Handler<Long>() {
                                @Override
                                public void handle(Long arg0) {
                                    sRequest.response().close();
                                }
                            });
                        }
                    });
                }
            })
                .setChunked(true);

            Pump.createPump(sRequest, cReq).start();

            sRequest.endHandler(new VoidHandler() {
                public void handle() {
                  cReq.end();
                }
              });

        }

        private int getChoice(int size) {
            int choice = (int) (Math.random() * (size - Float.MIN_VALUE));
            return choice;
        }
    }).listen(8080);
  }
}
