package org.apache.zeppelin.metadata;

import org.apache.zeppelin.metadata.element.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCache.class);

  private AtomicInteger SCHEMA_ALL = new AtomicInteger();
  private AtomicInteger SCHEMA_LOAD = new AtomicInteger();
  private AtomicInteger TABLE_ALL = new AtomicInteger();
  private AtomicInteger TABLE_LOAD = new AtomicInteger();
  private AtomicInteger DELETE_SCHEMA_COUNT = new AtomicInteger();
  private AtomicInteger DELETE_TABLE_COUNT = new AtomicInteger();
  private AtomicInteger DELETE_COLUMN_COUNT = new AtomicInteger();
  private long START_TIME = 0;

  private String databaseName;
  private String url;
  private ConnectionPool connectionPool;
  private ArrayList<Schema> schemas = new ArrayList<>();
  private SearchCache searchCache = new SearchCache();
  private ConcurrentHashMap<Integer, DatabaseElement> idsMap = new ConcurrentHashMap<>();
  private ArrayList<String> filter = new ArrayList<>();

  DatabaseCache(String databaseName, String url, String username, String password, String driver) {
    this.databaseName = databaseName;
    this.url = url;
    try {
      connectionPool = new ConnectionPool(username, password, url, driver);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    LOGGER.info("Create new DatabaseCache. Name: " + databaseName + "; url: " + url);
  }

  void updateDatabaseCache() {

    Thread statusThread = null;
    if (databaseName.equals("remote") && MetaSettings.REMOTE_LOG_ENABLE) {
      statusThread = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            TABLE_LOAD.set(TABLE_ALL.get());
            SCHEMA_LOAD.set(SCHEMA_ALL.get());
            break;
          } finally {
            LOGGER.info(String.format("### size: %7d | DELETED: |%4d|%4d|%4d|  | schema: %6.2f%% (%4d/%4d) | table: %6.2f%% (%6d/%6d) | sec %5d | available: %3d  busy: %3d",
                    idsMap.size(), DELETE_SCHEMA_COUNT.get(), DELETE_TABLE_COUNT.get(), DELETE_COLUMN_COUNT.get(),
                    (double) SCHEMA_LOAD.get() / SCHEMA_ALL.get() * 100, SCHEMA_LOAD.get(), SCHEMA_ALL.get(),
                    (double) TABLE_LOAD.get() / TABLE_ALL.get() * 100, TABLE_LOAD.get(), TABLE_ALL.get(),
                    (System.currentTimeMillis() - START_TIME) / 1000,
                    connectionPool.availableConn.size(), connectionPool.busyConn.size()));
          }
        }
      });
      statusThread.start();
    }

    START_TIME = System.currentTimeMillis();

    String defaultThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName("*" + databaseName + "*-updater-thread");
    LOGGER.info("Start updating \"" + databaseName + "\" from " + url);

    refreshAllSchemas();

    ExecutorService executorService = Executors.newFixedThreadPool(1);
//    ExecutorService executorService = Executors.newFixedThreadPool(Math.max(filter.size(), 1));

    if (filter.size() > 0) {
      for (String f : filter) {
        executorService.submit(() -> {
          String schemaPattern = f + "%";
          refreshTables(schemaPattern);
          refreshColumns(schemaPattern, null);
        });
      }
    } else {
      executorService.submit(() -> {
        refreshTables(null);
        refreshColumns(null, null);
      });
    }

    try {
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      if (statusThread != null) {
        statusThread.interrupt();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      if (statusThread != null) {
        statusThread.join();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      LOGGER.info("Complete updating \"" + databaseName + "\" from " + url + " | elements in database: " + idsMap.size() + " |   time: " + (System.currentTimeMillis() - START_TIME));
      Thread.currentThread().setName(defaultThreadName);
      TABLE_ALL.set(0);
      SCHEMA_ALL.set(0);
      TABLE_LOAD.set(0);
      SCHEMA_LOAD.set(0);
      DELETE_SCHEMA_COUNT.set(0);
      DELETE_TABLE_COUNT.set(0);
      DELETE_COLUMN_COUNT.set(0);
    }
  }

  private void refreshAllSchemas() {
    try (CPConnection connection = connectionPool.getConnection();
         ResultSet result = connection.getMetaData().getSchemas()) {

      ArrayList<String> schemasToRemove = new ArrayList<>();
      schemas.forEach((s) -> schemasToRemove.add(s.getName()));
      while (result.next()) {
        String schemaName = result.getString("TABLE_SCHEM");
        if (filter != null && filter.size() != 0) {
          boolean needLoad = false;
          for (String s : filter) {
            if (schemaName.startsWith(s)) {
              needLoad = true;
              break;
            }
          }
          if (!needLoad) {
            continue;
          }
        }

        if (schemasToRemove.contains(schemaName)) {
          schemasToRemove.remove(schemaName); // the scheme is still in the database. it does not need to be deleted from cache
        } else {
          Schema schema = new Schema(schemaName);
          schemas.add(schema);
          idsMap.put(schema.getId(), schema);
          searchCache.add(schema);
        }
      }

      for (String schemaName : schemasToRemove) {
        Schema schema = removeSchema(schemaName);
        idsMap.remove(schema.getId());
        DELETE_SCHEMA_COUNT.incrementAndGet();
        searchCache.remove(schema);
      }
    } catch (SQLException e) {
      LOGGER.error("Cannot refresh database. URL: " + url, e);
    }
    SCHEMA_ALL.set(schemas.size());
  }

  private void refreshTables(String schemaPattern) {
    try (CPConnection connection = connectionPool.getConnection();
         ResultSet resultSet =
                 connection.getMetaData().getTables(null, schemaPattern, null, null)) {

      Schema schema = null;
      ArrayList<String> deletedTables = new ArrayList<>();
      while (resultSet.next()) {
        String schemaName = resultSet.getString("TABLE_SCHEM");
        String tableName = resultSet.getString("TABLE_NAME");
        String tableDescription = resultSet.getString("REMARKS");

        if (schema != null && !schemaName.equals(schema.getName())) {
          for (String delTableName : deletedTables) {
            Table table = schema.removeTable(delTableName);
            idsMap.remove(table.getId());
            DELETE_TABLE_COUNT.incrementAndGet();
            searchCache.remove(table);

            LOGGER.warn("DELETE: " + schema.getName() + "." + table.getName());

          }
          deletedTables.clear();
          schema = null;

          SCHEMA_LOAD.incrementAndGet();
        }

        if (schema == null) {
          schema = getSchema(schemaName);
          if (schema == null) {
            continue;
          }
          schema.getAllTables().forEach(t -> deletedTables.add(t.getName()));
        }


        TABLE_ALL.incrementAndGet();


        if (deletedTables.contains(tableName)) {
          deletedTables.remove(tableName);
          schema.getTable(tableName).setDescription(tableDescription);
        } else {
          Table table = new Table(tableName, schema);
          table.setDescription(tableDescription);
          schema.addTable(table);
          idsMap.put(table.getId(), table);
          searchCache.add(table);

        }

        if (resultSet.isLast()) {
          for (String delTableName : deletedTables) {
            Table table = schema.removeTable(delTableName);
            DELETE_TABLE_COUNT.incrementAndGet();
            idsMap.remove(table.getId());
            searchCache.remove(table);
          }

          SCHEMA_LOAD.incrementAndGet();
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  private void refreshColumns(String schemaPattern, String tablePattern) {
    try (CPConnection connection = connectionPool.getConnection();
         ResultSet resultSet =
                 connection.getMetaData().getColumns(null, schemaPattern, tablePattern, null)) {

      Schema schema = null;
      Table table = null;
      ArrayList<String> deletedColumns = new ArrayList<>();
      while (resultSet.next()) {
        String columnName = resultSet.getString("COLUMN_NAME");
        String columnDescription = resultSet.getString("REMARKS");
        String columnType = resultSet.getString("TYPE_NAME");
        String tableName = resultSet.getString("TABLE_NAME");
        String schemaName = resultSet.getString("TABLE_SCHEM");

        if (schema != null && !schemaName.equals(schema.getName())) {
          schema = null;
          table = null;
        }

        if (table != null && !tableName.equals(table.getName())) {
          for (String delColumnName : deletedColumns) {
            Column column = table.removeColumn(delColumnName);
            DELETE_COLUMN_COUNT.incrementAndGet();
            idsMap.remove(column.getId());
            searchCache.remove(column);
          }
          deletedColumns.clear();
          table = null;

          TABLE_LOAD.incrementAndGet();
        }

        if (schema == null) {
          schema = getSchema(schemaName);
          if (schema == null) {
            continue;
          }
        }

        if (table == null) {
          table = schema.getTable(tableName);
          deletedColumns.clear();

          if (table == null) {
            table = new Table(tableName, schema);
            schema.addTable(table);
          } else {
            table.getAllColumns().forEach(c -> deletedColumns.add(c.getName()));
          }
        }

        if (deletedColumns.contains(columnName)) {
          deletedColumns.remove(columnName);
          Column column = table.getColumn(columnName);
          column.setDescription(columnDescription);
          column.setValueType(columnType);
        } else {
          Column column = new Column(columnName, table);
          column.setDescription(columnDescription);
          column.setValueType(columnType);
          table.addColumn(column);
          idsMap.put(column.getId(), column);
          searchCache.add(column);
        }

        if (resultSet.isLast()) {
          for (String delColumnName : deletedColumns) {
            Column column = table.removeColumn(delColumnName);
            idsMap.remove(column.getId());
            DELETE_COLUMN_COUNT.incrementAndGet();
            searchCache.remove(column);
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  void forceRefreshSchema(Integer elementId, boolean isRecursively) {
    Schema schema = getSchemaById(elementId);
    if (schema == null) {
      LOGGER.error("Schema  with id " + elementId + " not found.");
      return;
    }
    refreshTables(schema.getName());
    if (isRecursively) {
      refreshColumns(schema.getName(), null);
    }
  }

  void forceRefreshTable(Integer elementId, Integer schemaId) {
    Schema schema = getSchemaById(schemaId);
    if (schema == null) {
      LOGGER.error("Schema  with id " + schemaId + " not found.");
      return;
    }
    Table table = getSchemaById(schemaId).getTableById(elementId);
    if (table == null) {
      LOGGER.error("Table with id " + elementId + " not found.");
      return;
    }
    refreshColumns(table.getParentSchema().getName(), table.getName());
  }

  public void setFilter(ArrayList<String> filter) {
    this.filter = filter;
  }

  private Schema removeSchema(String schemaName) {
    Schema schema = getSchema(schemaName);
    schemas.remove(schema);
    return schema;
  }

  HashSet<Integer> searchElements(String searchString) {
    return searchCache.searchElements(searchString);
  }


  ArrayList<Schema> getAllSchemas() {
    return schemas;
  }

  private Schema getSchema(String schemaName) {
    for (Schema schema : schemas) {
      if (schema.getName().equals(schemaName)) {
        return schema;
      }
    }
    return null;
  }

  DatabaseElement getDatabaseElementById(int id) {
    return idsMap.get(id);
  }

  String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String toString() {
    return this.schemas.toString();
  }

  Schema getSchemaById(long id) {
    for (Schema schema : schemas) {
      if (schema.getId() == id) {
        return schema;
      }
    }
    return null;
  }

  class SearchCache {
    private final ConcurrentHashMap<String, StringInfo> searchMap = new ConcurrentHashMap<>();

    private void add(DatabaseElement element) {
      StringInfo info = searchMap.get(element.getName());
      if (info == null) {
        info = new StringInfo();
        searchMap.put(element.getName(), info);
      }
      info.addElement(element);
    }

    private void remove(DatabaseElement element) {
      StringInfo info = searchMap.get(element.getName());
      if (info == null) {
        return;
      }
      info.removeElement(element);
    }

    HashSet<Integer> searchElements(String searchString) throws IllegalArgumentException {
      ArrayList<Integer> resultIds = new ArrayList<>();
      int findCount = 0;

      for (Map.Entry<String, StringInfo> rec : searchMap.entrySet()) {
        String str = rec.getKey();

        if (str.contains(searchString)) {
          ArrayList<DatabaseElement> elements = rec.getValue().getAllElements();
          int needElemCount = Math.min(MetaSettings.JSTREE_SEARCH_LIMIT - findCount, elements.size());

          for (int i = 0; i < needElemCount; i++) {
            resultIds.addAll(elements.get(i).getParentsIds());
            findCount++;
          }
          if (findCount > MetaSettings.JSTREE_SEARCH_LIMIT) {
            break;
          }
        }
      }
      return new HashSet<>(resultIds);
    }
  }

  private class StringInfo {
    private ArrayList<DatabaseElement> dbElements = new ArrayList<>();

    ArrayList<DatabaseElement> getAllElements() {
      return dbElements;
    }

    void addElement(DatabaseElement element) {
      dbElements.add(element);
    }

    void removeElement(DatabaseElement element) {
      dbElements.remove(element);
    }
  }
}