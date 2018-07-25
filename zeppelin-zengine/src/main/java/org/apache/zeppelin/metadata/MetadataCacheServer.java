package org.apache.zeppelin.metadata;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterProperty;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.metadata.element.DatabaseElement;
import org.apache.zeppelin.metadata.element.Schema;
import org.apache.zeppelin.metadata.element.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetadataCacheServer {
  private static Logger LOGGER = LoggerFactory.getLogger(MetadataCacheServer.class);

  private HashMap<String, DatabaseCache> databases = new HashMap<>();
  private ScheduledExecutorService updateDatabaseCacheService;

  public MetadataCacheServer(ZeppelinConfiguration conf, InterpreterSettingManager interpreterSettingManager) {
    LOGGER.info("Init MetadataCacheServer");
    this.updateDatabaseCacheService = Executors.newScheduledThreadPool(MetaSettings.PARALLEL_UPDATE_DB_COUNT);

    // create database cache from jdbc interpreters
    if (MetaSettings.ENABLE_DATABASECACHE_FOR_INTERPRETER) {
      for (InterpreterSetting interpreterSetting : interpreterSettingManager.get()) {
        if (interpreterSetting.getGroup().equals("jdbc")) {
          String interpreterName = interpreterSetting.getName();
          String url = getProperty(interpreterSetting, "default.url");
          String username = getProperty(interpreterSetting, "default.user");
          String password = getProperty(interpreterSetting, "default.password");
          String driver = getProperty(interpreterSetting, "default.driver");

          if (isCorrectStrings(interpreterName, url, username, password, driver)) {
            createDatabaseCache(interpreterName, url, username, password, driver, null);
          } else {
            LOGGER.error("Failed to parse interpreter properties! interpreterName: " + interpreterName);
          }
        }
      }
    }

    // create database cache from config file database-meta.json
    try {
      JsonElement element = new JsonParser().parse(new FileReader(conf.getMetadataCacheServerSettingsPath()));
      JsonObject json = element.getAsJsonObject();
      json.entrySet().forEach(entry -> {
        JsonObject settings = (JsonObject) entry.getValue();
        String dbName = entry.getKey();
        String url = settings.get("url").getAsString();
        String username = settings.get("username").getAsString();
        String password = settings.get("password").getAsString();
        String driver = settings.get("driver").getAsString();

        ArrayList<String> filter = new ArrayList<>();
        settings.get("schema_filter").getAsJsonArray().forEach(f -> filter.add(f.getAsString()));
        createDatabaseCache(dbName, url, username, password, driver, filter);
      });
    } catch (FileNotFoundException e) {
      LOGGER.error("Configuration file '" + conf.getMetadataCacheServerSettingsPath() + "' not found");
    }
  }

  private void createDatabaseCache(String dbName, String url, String username,
                                   String password, String driver, ArrayList<String> filter) {

    if (databases.containsKey(dbName)) {
      LOGGER.error("DatabaseCache with name '" + dbName + "' already exists");
      return;
    }

    DatabaseCache db;
    try {
      db = new DatabaseCache(dbName, url, username, password, driver);
    } catch (ClassNotFoundException e) {
      LOGGER.error("Can't open driver " + driver, e);
      return;
    }
    if (filter != null) {
      db.setFilter(filter);
    }
    databases.put(db.getDatabaseName(), db);
    updateDatabaseCacheService.scheduleWithFixedDelay(db::updateDatabaseCache, 3,
            MetaSettings.CACHE_TTL, TimeUnit.SECONDS);
  }

  private boolean isCorrectStrings(String... strings) {
    for (String str : strings) {
      if (str == null || str.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private String getProperty(InterpreterSetting setting, String propertyName) {
    HashMap properties = (HashMap) setting.getProperties();
    InterpreterProperty property = (InterpreterProperty) properties.get(propertyName);
    return property.getValue().toString();
  }

  public ArrayList<String> getAllDatabasesNames() {
    ArrayList<String> dbNames = new ArrayList<>();
    for (DatabaseCache database : databases.values()) {
      dbNames.add(database.getDatabaseName());
    }
    return dbNames;
  }

  public ArrayList<JsonObject> jstreeGetChildren(String databaseName, Integer elementId, String type, Integer schemaId) throws IllegalArgumentException {
    ArrayList<JsonObject> jsonElements = new ArrayList<>();

    if (!isCorrectStrings(databaseName, type) || elementId == null) {
      throw new IllegalArgumentException();
    }

    DatabaseCache db = getDatabaseCache(databaseName);
    if (type != null) {
      if (type.equals("schema")) {
        Schema schema = db.getSchemaById(elementId);
        if (schema != null) {
          schema.getAllTables().forEach(t -> jsonElements.add(t.toJson()));
        }
        return jsonElements;
      }
      if (type.equals("table") && schemaId != null) {
        Schema schema = db.getSchemaById(schemaId);
        if (schema != null) {
          Table table = schema.getTableById(elementId);
          if (table != null) {
            table.getAllColumns().forEach(c -> jsonElements.add(c.toJson()));
          }
          return jsonElements;
        }
      }
    }
    return jsonElements;
  }

  public ArrayList<JsonObject> jstreeGetRootElements(String databaseName) throws IllegalArgumentException {
    ArrayList<JsonObject> jsonElements = new ArrayList<>();
    DatabaseCache db = getDatabaseCache(databaseName);
    db.getAllSchemas().forEach((key, value) -> jsonElements.add(value.toJson()));
    return jsonElements;
  }

  public HashSet<Integer> jstreeSearch(String databaseName, String searchString) throws IllegalArgumentException {
    DatabaseCache db = getDatabaseCache(databaseName);
    return db.searchElements(searchString);
  }

  public JsonObject jstreeMassload(String databaseName, ArrayList<Integer> ids) throws IllegalArgumentException {
    DatabaseCache db = getDatabaseCache(databaseName);
    JsonObject resJson = new JsonObject();

    for (Integer id : ids) {
      JsonArray list = new JsonArray();
      DatabaseElement dbElement = db.getDatabaseElementById(id);

      if (dbElement != null) {
        for (DatabaseElement elem : dbElement.getInnerElements()) {
          list.add(elem.toJson());
        }
        resJson.add(String.valueOf(id), list);
      }
    }
    return resJson;
  }

  private DatabaseCache getDatabaseCache(String databaseName) throws IllegalArgumentException {
    DatabaseCache db = databases.get(databaseName);
    if (db != null) {
      return db;
    } else {
      throw new IllegalArgumentException("Can't get databaseCache. Name: \"" + databaseName + "\"");
    }
  }

  public void jstreeRefreshElement(String databaseName, Integer elementId, Integer schemaId, boolean isRecursively) {

    if (!isCorrectStrings(databaseName) || elementId == null) {
      throw new IllegalArgumentException();
    }
    DatabaseCache db = getDatabaseCache(databaseName);
    try {
      if (schemaId == null) {
        db.forceRefreshSchema(elementId, isRecursively);
      } else {
        db.forceRefreshTable(elementId, schemaId);
      }
    } catch (Exception initError) {
      throw new IllegalArgumentException("Can't refresh element. ", initError);
    }
  }
}















