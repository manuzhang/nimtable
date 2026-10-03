/*
 * Copyright 2026 Nimtable
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RESTCatalogAdapterIntegrationTest {
    private static final String CATALOG_NAME = "test";

    @TempDir private Path warehouse;

    private HadoopCatalog backingCatalog;
    private RESTCatalogAdapter adapter;
    private RESTCatalog restCatalog;
    private Server server;

    @BeforeEach
    void startServer() throws Exception {
        backingCatalog = new HadoopCatalog(new Configuration(), warehouse.toUri().toString());
        adapter = new RESTCatalogAdapter(backingCatalog);

        ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/api");
        context.addServlet(
                new ServletHolder(new RESTCatalogServlet(adapter)),
                "/catalog/" + CATALOG_NAME + "/*");

        server = new Server(new InetSocketAddress("127.0.0.1", 0));
        server.setHandler(context);
        server.start();

        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        restCatalog = new RESTCatalog();
        restCatalog.setConf(new Configuration());
        restCatalog.initialize(
                CATALOG_NAME,
                Map.of(
                        CatalogProperties.URI,
                        "http://127.0.0.1:" + port + "/api/catalog/" + CATALOG_NAME));
    }

    @AfterEach
    void stopServer() throws Exception {
        if (restCatalog != null) {
            restCatalog.close();
        }
        if (server != null) {
            server.stop();
        }
        if (adapter != null) {
            adapter.close();
        }
        if (backingCatalog != null) {
            backingCatalog.close();
        }
    }

    @Test
    void roundTripsCatalogOperationsThroughRestCatalog() {
        Namespace namespace = Namespace.of("analytics");
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "events");
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "payload", Types.StringType.get()));

        restCatalog.createNamespace(namespace);

        assertTrue(restCatalog.namespaceExists(namespace));
        assertEquals(List.of(namespace), restCatalog.listNamespaces());

        Table table = restCatalog.createTable(tableIdentifier, schema);

        assertEquals(schema.asStruct(), table.schema().asStruct());
        assertTrue(restCatalog.tableExists(tableIdentifier));
        assertEquals(List.of(tableIdentifier), restCatalog.listTables(namespace));
        assertEquals(schema.asStruct(), restCatalog.loadTable(tableIdentifier).schema().asStruct());

        assertTrue(restCatalog.dropTable(tableIdentifier));
        assertTrue(restCatalog.dropNamespace(namespace));
        assertFalse(restCatalog.tableExists(tableIdentifier));
        assertFalse(restCatalog.namespaceExists(namespace));
    }
}
