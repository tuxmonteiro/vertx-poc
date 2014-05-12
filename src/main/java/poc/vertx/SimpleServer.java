package poc.vertx;

import java.util.HashMap;
import java.util.HashSet;

import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.HttpVersion;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

public class SimpleServer extends Verticle {

  public void start() {

      final JsonObject conf = container.config();
      final Boolean supportKeepAlive = conf.getBoolean("supportKeepAlive", true);
      final Long keepAliveTimeOut = conf.getLong("keepAliveTimeOut", 2000L);
      final Long keepAliveMaxRequest = conf.getLong("maxKeepAliveRequests", 100L);
      final Integer clientRequestTimeOut = conf.getInteger("clientRequestTimeOut", 60000);
      final Integer clientConnectionTimeOut = conf.getInteger("clientConnectionTimeOut", 60000);
      final Boolean clientForceKeepAlive = conf.getBoolean("clientForceKeepAlive", false);

      final HashSet<Client> clients = new HashSet<>();
      final HashSet<Client> clients2 = new HashSet<>();

      boolean local = conf.containsField("local");
      if (local) {
          clients.add(new Client("127.0.0.1:8081"));
          clients.add(new Client("127.0.0.1:8082"));
          clients2.add(new Client("127.0.0.1:8083"));
          clients2.add(new Client("127.0.0.1:8084"));
      } else {
          clients.add(new Client("10.248.92.35:80"));
          clients.add(new Client("10.248.92.36:80"));
          clients2.add(new Client("10.249.51.4:80"));
          clients2.add(new Client("10.249.51.5:80"));
      }

     final HashMap<String, HashSet<Client>> vhosts = new HashMap<>();
     vhosts.put("teste.qa02.globoi.com", clients);
     vhosts.put("teste2.qa02.globoi.com", clients2);

     final Handler<HttpServerRequest> handlerHttpServerRequest = new Handler<HttpServerRequest>() {
        @Override
        public void handle(final HttpServerRequest sRequest) {

            final Long requestTimeoutTimer = vertx.setTimer(clientRequestTimeOut, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    serverShowErrorAndClose(sRequest, new java.util.concurrent.TimeoutException());
                }
            });

            String headerHost = sRequest.headers().get("Host").split(":")[0];

            final boolean connectionKeepalive = sRequest.headers().contains("Connection") ?
                    (!sRequest.headers().get("Connection").equals("close")) && supportKeepAlive : 
                    sRequest.version().equals(HttpVersion.HTTP_1_1) && supportKeepAlive;

            final Client client = ((Client)vhosts.get(headerHost).toArray()[getChoice(clients.size())])
                    .setKeepAlive(connectionKeepalive||clientForceKeepAlive)
                    .setKeepAliveTimeOut(keepAliveTimeOut)
                    .setKeepAliveMaxRequest(keepAliveMaxRequest)
                    .setConnectionTimeout(clientConnectionTimeOut);

            final Handler<HttpClientResponse> handlerHttpClientResponse = new Handler<HttpClientResponse>() {

                    @Override
                    public void handle(HttpClientResponse cResponse) {

                        vertx.cancelTimer(requestTimeoutTimer);

                        // Pump cResponse => sResponse
                        sRequest.response().headers().set(cResponse.headers());
                        Pump.createPump(cResponse, sRequest.response()).start();

                        cResponse.endHandler(new VoidHandler() {
                            @Override
                            public void handle() {
                                sRequest.response().end();
                                if (connectionKeepalive) {
                                    if (client.isKeepAliveLimit()) {
                                        serverNormalClose(sRequest);
                                        client.close();
                                    }
                                } else {
                                    serverNormalClose(sRequest);
                                    if (!clientForceKeepAlive) {
                                        client.close();
                                    }
                                }
                            }
                        });

                        cResponse.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
//                                System.err.println(event.getMessage());
                                serverShowErrorAndClose(sRequest, event);
                                client.close();
                            }
                        });
                }
            };

            sRequest.response().setChunked(true);

            final HttpClientRequest cRequest = client.connect()
                    .request(sRequest.method(), sRequest.uri(),handlerHttpClientResponse)
                    .setChunked(true);

            // Pump sRequest => cRequest
            cRequest.headers().set(sRequest.headers());
            if (clientForceKeepAlive && cRequest.headers().contains("Connection")) {
                cRequest.headers().set("Connection", "keep-alive");
            }
            Pump.createPump(sRequest, cRequest).start();

            cRequest.exceptionHandler(new Handler<Throwable>() {
                @Override
                public void handle(Throwable event) {
                    System.err.println(event.getMessage());
                    serverShowErrorAndClose(sRequest, event);
                    client.close();
                }
             });

            sRequest.endHandler(new VoidHandler() {
                @Override
                public void handle() {
                  cRequest.end();
                }
             });
        }
    };

    vertx.createHttpServer().requestHandler(handlerHttpServerRequest)
        .setTCPKeepAlive(conf.getBoolean("serverTCPKeepAlive",true))
        .listen(conf.getInteger("port",8080));

  }

  private int getChoice(int size) {
      int choice = (int) (Math.random() * (size - Float.MIN_VALUE));
      return choice;
  }

  private void serverShowErrorAndClose(final HttpServerRequest sRequest, final Throwable event) {

      if (event instanceof java.util.concurrent.TimeoutException) {
          sRequest.response().setStatusCode(504);
          sRequest.response().setStatusMessage("Gateway Time-Out");
      } else {
          sRequest.response().setStatusCode(502);
          sRequest.response().setStatusMessage("Bad Gateway");
      }

      try {
          sRequest.response().end();
      } catch (java.lang.IllegalStateException e) {
          // Response has already been written ?
//          System.err.println(e.getMessage());
      }

      try {
          sRequest.response().close();
      } catch (RuntimeException e) {
          // Socket null or already closed
//          System.err.println(e.getMessage());
      }
  }

  private void serverNormalClose(final HttpServerRequest sRequest) {
      try {
          sRequest.response().close();
      } catch (Exception e) {} // Ignore "Already Closed" error
  }

  class Client {

      private HttpClient client;
      private String host;
      private Integer port;
      private Integer timeout;

      private boolean keepalive;
      private Long keepAliveMaxRequest;
      private Long keepAliveTimeMark;
      private Long keepAliveTimeOut;
      private Long requestCount;

      public Client(final String hostWithPort) {
          this.client = null;
          this.host = hostWithPort.split(":")[0];
          this.port = Integer.parseInt(hostWithPort.split(":")[1]);
          this.timeout = 60000;
          this.keepalive = true;
          this.keepAliveMaxRequest = Long.MAX_VALUE-1;
          this.keepAliveTimeMark = System.currentTimeMillis();
          this.keepAliveTimeOut = 86400000L; // One day
          this.requestCount = 0L;
      }

      public Client() {
          this("127.0.0.1:80");
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

      public Integer getConnectionTimeout() {
          return timeout;
      }

      public Client setConnectionTimeout(Integer timeout) {
          this.timeout = timeout;
          return this;
      }

      public boolean isKeepalive() {
          return keepalive;
      }

      public Client setKeepAlive(boolean keepalive) {
          this.keepalive = keepalive;
          return this;
      }

      public Long getKeepAliveRequestCount() {
        return requestCount;
      }

      public Long getMaxRequestCount() {
        return keepAliveMaxRequest;
      }

      public Client setKeepAliveMaxRequest(Long maxRequestCount) {
        this.keepAliveMaxRequest = maxRequestCount;
        return this;
      }

      public Long getKeepAliveTimeOut() {
          return keepAliveTimeOut;
      }

      public Client setKeepAliveTimeOut(Long keepAliveTimeOut) {
          this.keepAliveTimeOut = keepAliveTimeOut;
          return this;
      }

      public boolean isKeepAliveLimit() {
          Long now = System.currentTimeMillis();
          if (requestCount<=keepAliveMaxRequest) {
              requestCount++;
          }
          if ((requestCount>=keepAliveMaxRequest) || ((keepAliveTimeMark+keepAliveTimeOut)<now)) {
              keepAliveTimeMark = now;
              requestCount = 0L;
              return true;
          }
          return false;
      }

      // Lazy initialization
      public HttpClient connect() {
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
//                    System.err.println(e.getMessage());
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
//                  System.err.println(e.getMessage());
              } finally {
                  client=null;
              }
          }
      }
  }

}