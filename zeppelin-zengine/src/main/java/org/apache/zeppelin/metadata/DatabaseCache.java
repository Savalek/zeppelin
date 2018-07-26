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
  private ConcurrentHashMap<String, Schema> schemas = new ConcurrentHashMap<>();
  private SearchCache searchCache = new SearchCache();
  private ConcurrentHashMap<Integer, DatabaseElement> idsMap = new ConcurrentHashMap<>();
  private ArrayList<String> filter = new ArrayList<>();

  DatabaseCache(String databaseName, String url, String username, String password, String driver) throws ClassNotFoundException {
    this.databaseName = databaseName;
    this.url = url;
    connectionPool = new ConnectionPool(username, password, url, driver);
    LOGGER.info("Create new DatabaseCache. Name: {}; url: {}.", databaseName, url);
  }

  void updateDatabaseCache() {

    Thread statusThread = null;
    if (databaseName.equals("remote_conf") && MetaSettings.REMOTE_LOG_ENABLE) {
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
    LOGGER.info("Start updating '{}' from '{}'", databaseName, url);

    refreshAllSchemas();

    ExecutorService executorService = Executors.newFixedThreadPool(Math.max(filter.size(), 1));

    if (filter.size() > 0) {
      for (String f : filter) {
        executorService.submit(() -> {
          refreshTables(f);
          refreshColumns(f, null);
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
      LOGGER.error("Can't shutdown executorService", e);
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

      schemas.forEach((key, value) -> value.setRelevant(false));

      while (result.next()) {
        String schemaName = result.getString("TABLE_SCHEM");
        if (filter != null && filter.size() != 0) {
          boolean needLoad = false;
          for (String f : filter) {
            if (schemaName.matches(f.replace("%", ".*"))) {
              needLoad = true;
              break;
            }
          }
          if (!needLoad) {
            continue;
          }
        }

        Schema schema = getSchema(schemaName);
        if (schema == null) {
          schema = new Schema(schemaName);
          schemas.put(schemaName, schema);
          idsMap.put(schema.getId(), schema);
          searchCache.add(schema);
        }
        schema.setRelevant(true);
      }

      Iterator<Map.Entry<String, Schema>> iterator = schemas.entrySet().iterator();
      while (iterator.hasNext()) {
        Schema schema = iterator.next().getValue();
        if (!schema.isRelevant()) {
          idsMap.remove(schema.getId());
          searchCache.remove(schema);
          iterator.remove();
          DELETE_SCHEMA_COUNT.incrementAndGet();
        }
      }

      SCHEMA_ALL.set(schemas.size());
    } catch (SQLException e) {
      LOGGER.error("Can't refresh schemas from {}", url, e);
    }
  }

  private void refreshTables(String schemaPattern) {
    String regularSchema = schemaPattern.replaceAll("%", ".*");

    // list of schemas by filter
    ArrayList<Schema> schemasList = new ArrayList<>();
    getAllSchemas().entrySet().stream()
            .filter(entry -> entry.getKey().matches(regularSchema))
            .forEach(entry -> {
              schemasList.add(entry.getValue());
              entry.getValue().getAllTables().forEach(t -> t.setRelevant(false));
            });

    try (CPConnection connection = connectionPool.getConnection();
         ResultSet resultSet = connection.getMetaData().getTables(null, schemaPattern, null, null)) {

      Schema schema = null;
      while (resultSet.next()) {
        String schemaName = resultSet.getString("TABLE_SCHEM");
        String tableName = resultSet.getString("TABLE_NAME");
        String tableDescription = resultSet.getString("REMARKS");

        if (schema != null && !schemaName.equals(schema.getName())) {
          schema = null;
        }

        if (schema == null) {
          schema = getSchema(schemaName);
          if (schema == null) {
            continue;
          }
          SCHEMA_LOAD.incrementAndGet();
        }

        TABLE_ALL.incrementAndGet();
        Table table = schema.getTable(tableName);
        if (table == null) {
          table = new Table(tableName, schema);
          schema.addTable(table);
          idsMap.put(table.getId(), table);
          searchCache.add(table);
        }

        table.setRelevant(true);
        table.setDescription(tableDescription);
      }
    } catch (SQLException e) {
      LOGGER.error("Can't refresh table from {}", url, e);
    }

    schemasList.forEach((schema -> {
      Iterator<Table> iterator = schema.getAllTables().iterator();
      while (iterator.hasNext()) {
        Table table = iterator.next();
        if (!table.isRelevant()) {
          iterator.remove();
          DELETE_TABLE_COUNT.incrementAndGet();
          idsMap.remove(table.getId());
          searchCache.remove(table);
        }
      }
    }));
  }


  private void refreshColumns(String schemaPattern, String tablePattern) {
    String regularSchema = schemaPattern == null ? ".*" : schemaPattern.replace("%", ".*");
    String regularTable = tablePattern == null ? ".*" : tablePattern.replace("%", ".*");

    // list of tables by filter
    ArrayList<Table> tableList = new ArrayList<>();
    getAllSchemas().entrySet().stream()
            .filter(entry -> entry.getKey().matches(regularSchema))
            .map(Map.Entry::getValue)
            .flatMap(schema -> schema.getAllTables().stream())
            .filter(table -> table.getName().matches(regularTable))
            .forEach(table -> {
              tableList.add(table);
              table.getAllColumns().forEach(column -> column.setRelevant(false));
            });

    try (CPConnection connection = connectionPool.getConnection();
         ResultSet resultSet = connection.getMetaData().getColumns(null, schemaPattern, tablePattern, null)) {
      Schema schema = null;
      Table table = null;
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
          table = null;
        }

        if (schema == null) {
          schema = getSchema(schemaName);
          if (schema == null) {
            continue;
          }
        }

        if (table == null) {
          table = schema.getTable(tableName);
          if (table == null) {
            continue;
          }
          TABLE_LOAD.incrementAndGet();
        }

        Column column = table.getColumn(columnName);
        if (column == null) {
          column = new Column(columnName, table);
          table.addColumn(column);
          idsMap.put(column.getId(), column);
          searchCache.add(column);
        }
        column.setRelevant(true);
        column.setDescription(columnDescription);
        column.setValueType(columnType);
      }
    } catch (SQLException e) {
      LOGGER.error("Can't refresh columns from {}", url, e);
    }

    tableList.forEach(table -> {
      Iterator<Column> iterator = table.getAllColumns().iterator();
      while (iterator.hasNext()) {
        Column column = iterator.next();
        if (!column.isRelevant()) {
          iterator.remove();
          idsMap.remove(column.getId());
          DELETE_COLUMN_COUNT.incrementAndGet();
          searchCache.remove(column);
        }
      }
    });
  }


  void forceRefreshSchema(Integer elementId, boolean isRecursively) {
    Schema schema = getSchemaById(elementId);
    if (schema == null) {
      LOGGER.error("Schema with id {} not found.", elementId);
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
      LOGGER.error("Schema with id {} not found.", schemaId);
      return;
    }
    Table table = getSchemaById(schemaId).getTableById(elementId);
    if (table == null) {
      LOGGER.error("Table with id not found.", elementId);
      return;
    }
    refreshColumns(table.getParentSchema().getName(), table.getName());
  }

  public void setFilter(ArrayList<String> filter) {
    this.filter = filter;
  }

  HashSet<Integer> searchElements(String searchString) {
    return searchCache.searchElements(searchString);
  }


  ConcurrentHashMap<String, Schema> getAllSchemas() {
    return schemas;
  }

  private Schema getSchema(String schemaName) {
    return schemas.get(schemaName);
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

  Schema getSchemaById(int id) {
    DatabaseElement element = idsMap.get(id);
    if (element instanceof Schema) {
      return (Schema) element;
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