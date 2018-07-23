package org.apache.zeppelin.metadata;

public class MetaSettings {
  public static final int RES_POOL_MIN_CONNECTIONS = 2;
  public static final int RES_POOL_MAX_CONNECTIONS = 40;
  public static final int RES_POOL_CONNECTION_TTL = 15;
  public static final int CACHE_TTL = 600;
  public static final int JSTREE_SEARCH_LIMIT = 20;
  public static final int PARALLEL_UPDATE_DB_COUNT = 4;

  public static final boolean REMOTE_LOG_ENABLE = true;
  public static final boolean ENABLE_DATABASECACHE_FOR_INTERPRETER = false;
}
