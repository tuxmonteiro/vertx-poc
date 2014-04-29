package poc.serveronly;
/*
 * Copyright 2013 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */

import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

public class SimpleServer extends Verticle {

  public void start() {

     final HttpClient client = vertx.createHttpClient().setHost("10.249.51.2").setPort(80);
//    final HttpClient client = vertx.createHttpClient().setHost("127.0.0.1").setPort(8081);
//      final HttpClient client = vertx.createHttpClient().setHost("172.20.0.2").setPort(80);

      HttpServer server =  vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

        @Override
        public void handle(final HttpServerRequest sRequest) {
            sRequest.response().setChunked(true);

            final HttpClientRequest cReq = client.request(sRequest.method(), sRequest.uri(),
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
//                .setWriteQueueMaxSize(65536);

            Pump.createPump(sRequest, cReq).start();

            sRequest.endHandler(new VoidHandler() {
                public void handle() {
                  cReq.end();
                }
              });

        }
    });
//    server.setTCPKeepAlive(false).listen(8080);
   server
//       .setReceiveBufferSize(1024*1024)
//       .setSoLinger(1)
       .listen(8080);
  }
}
