/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql;

import org.apache.avro.Schema;
import org.apache.samza.sql.api.data.Data;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class Utils {

  public static final String HACK_STREAM_HANDLER_SYSTEM_PROPERTY = "hackStreamHandlerProperty";

  /**
   * Load Avro schema from classpath.
   *
   * @param schemaUrl Avro schema url
   * @return avro schema corresponding to the schema url
   * @throws IOException if avro schema cannot be loaded
   */
  public static Schema loadAvroSchemaFromClassPath(String schemaUrl) throws IOException {

    // TODO: remove this once we have a proper way of managing metadata related schemas and calcite models
    if (System.getProperty(HACK_STREAM_HANDLER_SYSTEM_PROPERTY) == null) {
      URL.setURLStreamHandlerFactory(new org.apache.samza.sql.Utils.ResourceStreamHandlerFactory());
      System.setProperty(HACK_STREAM_HANDLER_SYSTEM_PROPERTY, "alreadyWorkedAroundTheEvilJDK");
    }

    URL avroSchema = new URL(schemaUrl);
    URLConnection connection = avroSchema.openConnection();

    return new Schema.Parser().parse(connection.getInputStream());
  }

  /**
   * Defines a <code>URLStreamHandler</code> which reads resources from class path.
   */
  public static class ResourceHandler extends URLStreamHandler {

    private final ClassLoader classLoader;

    public ResourceHandler() {
      this.classLoader = getClass().getClassLoader();
    }

    public ResourceHandler(ClassLoader classLoader) {
      this.classLoader = classLoader;
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
      URL resourceUrl = classLoader.getResource(u.getPath());

      if (resourceUrl == null) {
        return null;
      }

      return resourceUrl.openConnection();
    }
  }

  /**
   * Defines a <code>URLStreamHandlerFactory</code> which implements support for
   * 'resource:' protocol.
   */
  public static class ResourceStreamHandlerFactory implements URLStreamHandlerFactory {

    public static final String PROTOCOL = "resource";

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
      return protocol.equals(PROTOCOL) ? new ResourceHandler() : null;
    }
  }

  public static boolean isStruct(Data data){
    return data.schema().getType() == org.apache.samza.sql.api.data.Schema.Type.STRUCT;
  }
}
