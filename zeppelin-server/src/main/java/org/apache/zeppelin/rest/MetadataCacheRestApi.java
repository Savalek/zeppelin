package org.apache.zeppelin.rest;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.metadata.MetaSettings;
import org.apache.zeppelin.metadata.MetadataCacheServer;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.ArrayList;

@Path("/metadatacache")
@Produces("application/json")
public class MetadataCacheRestApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCacheRestApi.class);

  private MetadataCacheServer server;

  public MetadataCacheRestApi(MetadataCacheServer metadataServer) {
    this.server = metadataServer;
  }


  /**
   * @return name's list of all available database
   */
  @GET
  @Path("/jstree/databases_list")
  @ZeppelinApi
  public Response getAllDatabases() {
    return new JsonResponse<>(Response.Status.OK, server.getAllDatabasesNames()).build();
  }

  /**
   * @return json list of elementId's children
   */
  @GET
  @Path("/jstree/get_children")
  @ZeppelinApi
  public Response jstreeGetChildren(@QueryParam("database") String databaseName,
                                    @QueryParam("id") String elementId,
                                    @QueryParam("type") String type,
                                    @QueryParam("schemaId") String schemaId) {

    try {
      String answer;
      if (elementId.equals("#")) {
        answer = server.jstreeGetRootElements(databaseName).toString();
      } else {
        Integer iElementId = getValue(elementId);
        Integer iSchemaId = getValue(schemaId);
        answer = server.jstreeGetChildren(databaseName, iElementId, type, iSchemaId).toString();
      }
      Response.ResponseBuilder r = Response.status(Response.Status.OK).entity(answer);
      return r.build();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid parameters!", e);
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * @return json list of elementId's children
   */
  @GET
  @Path("/jstree/refresh_element")
  @ZeppelinApi
  public Response jstreeRefreshElement(@QueryParam("database") String databaseName,
                                       @QueryParam("id") String elementId,
                                       @QueryParam("schemaId") String schemaId,
                                       @QueryParam("recursively") Boolean isRecursively) {

    try {
      isRecursively = isRecursively == null ? false : isRecursively;
      server.jstreeRefreshElement(databaseName, getValue(elementId),
              getValue(schemaId), isRecursively);
      return Response.status(Response.Status.OK).build();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid parameters!", e);
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * Massload method for JsTree
   */
  @GET
  @Path("/jstree/massload")
  @ZeppelinApi
  public Response jstreeMassload(@QueryParam("database") String databaseName,
                                 @QueryParam("ids") String elementIds) {

    String[] strIds = elementIds.split(",");
    ArrayList<Integer> ids = new ArrayList<>(strIds.length);
    for (String strId : strIds) {
      Integer id = getValue(strId);
      if (id != null) {
        ids.add(id);
      }
    }
    try {
      String answer = server.jstreeMassload(databaseName, ids).toString();
      Response.ResponseBuilder r = Response.status(Response.Status.OK).entity(answer);
      return r.build();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid parameters!", e);
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * Search method for JsTree
   */
  @GET
  @Path("/jstree/search")
  @ZeppelinApi
  public Response jstreeSearch(@QueryParam("database") String databaseName,
                               @QueryParam("str") String searchString) {

    try {
      String answer = server.jstreeSearch(databaseName, searchString).toString();
      Response.ResponseBuilder r = Response.status(Response.Status.OK).entity(answer);
      return r.build();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid parameters!", e);
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * Search method for JsTree
   */
  @GET
  @Path("/jstree/get_search_limit")
  @ZeppelinApi
  public Response jstreeSearchLimit() {
    int limit = MetaSettings.JSTREE_SEARCH_LIMIT;
    Response.ResponseBuilder r = Response.status(Response.Status.OK).entity(limit);
    return r.build();
  }


  private Integer getValue(String str) {
    try {
      return Integer.parseInt(str);
    } catch (NumberFormatException e) {
      return null;
    }
  }

}
