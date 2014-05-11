package poc.vertx;

import java.util.HashMap;
import java.util.HashSet;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
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

      final JsonObject conf = container.config();
      final Integer timeOutPosEnd = conf.getInteger("timeOutPosEnd", 2000);

      final HashSet<Client> clients = new HashSet<>();
      final HashSet<Client> clients2 = new HashSet<>();

      boolean local = conf.containsField("local");
      if (local) {
          clients.add(new Client().setHost("127.0.0.1").setPort(8081));
          clients.add(new Client().setHost("127.0.0.1").setPort(8082));
          clients2.add(new Client().setHost("127.0.0.1").setPort(8083));
          clients2.add(new Client().setHost("127.0.0.1").setPort(8084));
      } else {
          clients.add(new Client().setHost("10.248.92.35").setPort(80));
          clients.add(new Client().setHost("10.248.92.36").setPort(80));
          clients2.add(new Client().setHost("10.249.51.4").setPort(80));
          clients2.add(new Client().setHost("10.249.51.5").setPort(80));
      }

     final HashMap<String, HashSet<Client>> vhosts = new HashMap<>();
     vhosts.put("teste.qa02.globoi.com", clients);
     vhosts.put("teste2.qa02.globoi.com", clients2);

      vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
        @Override
        public void handle(final HttpServerRequest sRequest) {
            String headerHost = sRequest.headers().get("Host").split(":")[0];
            final boolean connectionKeepalive = sRequest.headers().contains("Connection") ? 
                    !sRequest.headers().get("Connection").equals("close") : true;
            final Client client = ((Client) vhosts.get(headerHost).toArray()[getChoice(clients.size())])
                    .setKeepAlive(connectionKeepalive)
                    .setTimeout(timeOutPosEnd);
            final Handler<HttpClientResponse> handlerHttpClientResponse = new Handler<HttpClientResponse>() {
                    @Override
                    public void handle(HttpClientResponse cResponse) {
                        sRequest.response().headers().set(cResponse.headers());
                        Pump.createPump(cResponse, sRequest.response()).start();
                        cResponse.endHandler(new VoidHandler() {
                            public void handle() {
                                sRequest.response().end();
                                if (!connectionKeepalive) {
                                    vertx.setTimer(timeOutPosEnd, new Handler<Long>() {
                                        @Override
                                        public void handle(Long event) {
                                            try {
                                                sRequest.response().close();
                                            } catch (RuntimeException e) {
                                                System.err.println(e.getMessage());
                                            }
                                            client.close();
                                        }
                                    });
                                }
                            }
                        });
                        cResponse.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                System.err.println(event.getMessage());
                                serverShowErrorAndClose(sRequest, event);
                            }
                        });
                }
            };
            sRequest.response().setChunked(true);
            final HttpClientRequest cRequest = client.getClient()
                    .request(sRequest.method(), sRequest.uri(),handlerHttpClientResponse)
                    .setChunked(true);
            cRequest.headers().set(sRequest.headers());
            Pump.createPump(sRequest, cRequest).start();
            cRequest.exceptionHandler(new Handler<Throwable>() {
                @Override
                public void handle(Throwable event) {
                    System.err.println(event.getMessage());
                    serverShowErrorAndClose(sRequest, event);
                }
            });
            sRequest.endHandler(new VoidHandler() {
                public void handle() {
                  cRequest.end();
                }
              });
        }
    })
    .setTCPKeepAlive(conf.getBoolean("serverTCPKeepAlive",true))
    .listen(conf.getInteger("port",8080));
  }

  private int getChoice(int size) {
      int choice = (int) (Math.random() * (size - Float.MIN_VALUE));
      return choice;
  }

  private void serverShowErrorAndClose(final HttpServerRequest sRequest, final Throwable event) {
      if (event.getCause() instanceof java.util.concurrent.TimeoutException) {
          sRequest.response().setStatusCode(504);
          sRequest.response().setStatusMessage("Gateway Time-Out");
      } else {
          sRequest.response().setStatusCode(502);
          sRequest.response().setStatusMessage("Bad Gateway");
      }
      sRequest.response().end();
      vertx.setTimer(100L, new Handler<Long>() {
        @Override
        public void handle(Long event) {
            try {
                sRequest.response().close();
            } catch (RuntimeException e) {
                System.err.println(e.getMessage());
            }
        }
      });

  }

  private class Client {

      HttpClient client;
      String host;
      Integer port;
      boolean keepalive;
      Integer timeout;

      public Client() {
          this.client = null;
          this.host = "127.0.0.1";
          this.port = 80;
          this.keepalive = true;
          this.timeout = 60000;
      }

      public String getHost() {
          return host;
      }

      public Client setHost(String host) {
          this.host = host;
          return this;
      }

      public Integer getPort() {
          return port;
      }

      public Client setPort(Integer port) {
          this.port = port;
          return this;
      }

      public boolean isKeepalive() {
          return keepalive;
      }

      public Client setKeepAlive(boolean keepalive) {
          this.keepalive = keepalive;
          return this;
      }

      public Integer getTimeout() {
          return timeout;
      }

      public Client setTimeout(Integer timeout) {
          this.timeout = timeout;
          return this;
      }

      // Lazy initialization
      public HttpClient getClient() {
          if (client==null) {
              client = vertx.createHttpClient()
                  .setKeepAlive(keepalive)
                  .setTCPKeepAlive(keepalive)
                  .setConnectTimeout(timeout)
                  .setHost(host)
                  .setPort(port);
              client.exceptionHandler(new Handler<Throwable>() {
                @Override
                public void handle(Throwable e) {
                    System.err.println(e.getMessage());
                }
              });
          }
          return client;
      }

      public void close() {
          if (client!=null) {
              try {
                  client.close();
              } catch (IllegalStateException e) {
                  // Already closed
                  System.err.println(e.getMessage());
              }          }
          client=null;
      }
  }
}