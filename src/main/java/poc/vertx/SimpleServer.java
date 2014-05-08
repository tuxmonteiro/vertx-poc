package poc.vertx;

import java.util.HashMap;
import java.util.HashSet;

import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

public class SimpleServer extends Verticle {

  public void start() {

      boolean local = true;
      final JsonObject conf = container.config();

      final HashSet<HttpClient> clients = new HashSet<>();
      final HashSet<HttpClient> clients2 = new HashSet<>();

      final boolean clientKeepalive = conf.containsField("clientKeepAliveState") && !conf.getString("clientKeepAliveState").equals("close");

      if (local) {
          clients.add(vertx.createHttpClient().setHost("127.0.0.1").setPort(8081)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
          clients.add(vertx.createHttpClient().setHost("127.0.0.1").setPort(8082)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
          clients2.add(vertx.createHttpClient().setHost("127.0.0.1").setPort(8083)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
          clients2.add(vertx.createHttpClient().setHost("127.0.0.1").setPort(8084)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
      } else {
          clients.add(vertx.createHttpClient().setHost("10.248.92.35").setPort(80)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
          clients.add(vertx.createHttpClient().setHost("10.248.92.36").setPort(80)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
          clients2.add(vertx.createHttpClient().setHost("10.249.51.4").setPort(80)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
          clients2.add(vertx.createHttpClient().setHost("10.249.51.5").setPort(80)
                  .setKeepAlive(clientKeepalive)
                  .setConnectTimeout(conf.getInteger("clientTimeout", 10000))
                  .setReuseAddress(clientKeepalive)
                  .setTCPKeepAlive(clientKeepalive));
      }

     final HashMap<String, HashSet<HttpClient>> vhosts = new HashMap<>();
     vhosts.put("teste.qa02.globoi.com", clients);
     vhosts.put("teste2.qa02.globoi.com", clients2);

      vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

        @Override
        public void handle(final HttpServerRequest sRequest) {
            sRequest.response().setChunked(true);

            String header = sRequest.headers().get("Host").split(":")[0];
            final HttpClientRequest cReq = ((HttpClient) vhosts.get(header).toArray()[getChoice(clients.size())])
                    .request(sRequest.method(), sRequest.uri(), new Handler<HttpClientResponse>() {
                        @Override
                        public void handle(final HttpClientResponse cResponse) {

                            final Handler<Void> cEndHandler = new VoidHandler() {
                                public void handle() {
                                    sRequest.response().end();
                                    try {
                                        sRequest.response().closeHandler(new Handler<Void>() {
                                            @Override
                                            public void handle(Void event) {
                                                cResponse.netSocket().close();
                                            }
                                        });
                                    } catch (IllegalStateException e) {
                                        // Ignore "double" close
                                        return;
                                    }
                                }
                            };

                            Pump.createPump(cResponse, sRequest.response()).start();

                            cResponse.endHandler(cEndHandler);
                        }
                    }).setChunked(true);

            cReq.exceptionHandler(new Handler<Throwable>() {
                @Override
                public void handle(Throwable event) {
                    if (event.getCause() instanceof java.util.concurrent.TimeoutException) {
                        sRequest.response().setStatusCode(504);
                        sRequest.response().setStatusMessage("Gateway Time-Out");
                        sRequest.response().end();
                        return;
                    }
                    sRequest.response().close();
                }
            });

            Pump.createPump(sRequest, cReq).start();

            sRequest.endHandler(new VoidHandler() {
                public void handle() {
                    if (conf.containsField("clientTimeout")) {
                        cReq.setTimeout(conf.getLong("clientTimeout"));
                    }
                    cReq.end();
                    vertx.setTimer(conf.getLong("serverTimeout",10000L), new Handler<Long>() {
                        @Override
                        public void handle(Long arg0) {
                            try {
                                sRequest.response().close();
                            } catch (RuntimeException r) {
                                return;
                            }
                        }
                    });

                }
            });

        }

        private int getChoice(int size) {
            int choice = (int) (Math.random() * (size - Float.MIN_VALUE));
            return choice;
        }
    }).listen(conf.getInteger("port",8080));
  }
}
