package com.starrocks.http.rest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.starrocks.analysis.*;
import com.starrocks.common.DdlException;
import com.starrocks.http.*;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.*;
import com.starrocks.sql.parser.SqlParser;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.*;

/**
 * @Description TODO
 * @Auther tao.zhou
 * @Date 2023-3-22 19:14
 * @Version 1.0
 */
public class SqlParserAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(SqlParserAction.class);

    private static final String PARAM = "sql";

    public SqlParserAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/sql/parse", new SqlParserAction(controller));
    }

    Gson gson = new Gson();

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        response.setContentType("application/json");
        RestResult result = new RestResult();

        final String content = request.getContent();
        if (StringUtils.isEmpty(content)) {
            sendErrorResult(result, request, response);
        }

        final SqlParameter sqlParameter = gson.fromJson(content, SqlParameter.class);
        final String orgSql = sqlParameter.getSql();

        if (StringUtils.isEmpty(orgSql)) {
            sendErrorResult(result, request, response);
        } else {
            final RelationResponse parse = getRels(orgSql);
            result.addResultEntry("result", parse);
            sendResult(request, response, result);
        }
    }


    static class SqlParameter {
        private String sql;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }
    }

    private void sendErrorResult(RestResult result, BaseRequest request, BaseResponse response) {
        result.addResultEntry("error", "sql param is null, please set valid param!");
        response.appendContent(result.toJson());
        writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
    }


    private static RelationResponse getRels(String sql) {
        List<StatementBase> parse = SqlParser.parse(sql, 32);
        List<Rel> list = new ArrayList<>();
        List<String> cant = new ArrayList<>();
        RelationResponse res = new RelationResponse();
        for (StatementBase stmt : parse) {
            try {
                list.addAll(parse(stmt));
            } catch (Exception e) {
                cant.add(e.getMessage());
            }
        }
        res.setRelation(list);
        res.setCant(cant);
        return res;
    }

    static class RelationResponse {
        List<Rel> relation;
        List<String> cant;

        public List<Rel> getRelation() {
            return relation;
        }

        public void setRelation(List<Rel> relation) {
            this.relation = relation;
        }

        public List<String> getCant() {
            return cant;
        }

        public void setCant(List<String> cant) {
            this.cant = cant;
        }
    }

    static class Rel {
        String from;
        String to;
        String type;

        public Rel(String from, String to, String type) {
            this.from = from;
            this.to = to;
            this.type = type;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }


    private static List<Rel> parse(StatementBase st) {
        List<Rel> list = new ArrayList<>();
        if (st instanceof LoadStmt) {
            LoadStmt ls = (LoadStmt) st;
            for (DataDescription dd : ls.getDataDescriptions()) {
                for (String filePath : dd.getFilePaths()) {
                    list.add(new Rel(filePath, dd.getTableName(), "LOAD"));
                }
            }
        } else if (st instanceof InsertStmt) {
            String to = ((InsertStmt) st).getTableName().toString();
            for (String from : getQuery(((InsertStmt) st).getQueryStatement().getQueryRelation())) {
                list.add(new Rel(from, to, "INSERT"));
            }
        } else if (st instanceof QueryStatement) {
            for (String from : getQuery(((QueryStatement) st).getQueryRelation())) {
                list.add(new Rel(from, null, null));
            }
        } else if (st instanceof TruncateTableStmt) {
            String to = ((TruncateTableStmt) st).getTblRef().toString();
            list.add(new Rel(null, to, "TRUNCATE"));
        } else if (st instanceof DeleteStmt) {
            String to = ((DeleteStmt) st).getTableName().toString();
            list.add(new Rel(null, to, "DELETE"));
        } else if (st instanceof UpdateStmt) {
            String to = ((UpdateStmt) st).getTableName().toString();
            list.add(new Rel(null, to, "UPDATE"));
        } else if (st instanceof CreateTableStmt) {
            String to = ((CreateTableStmt) st).getDbTbl().toString();
            list.add(new Rel(null, to, "CREATE"));
        } else if (st instanceof DropTableStmt) {
            String to = ((DropTableStmt) st).getTableNameObject().toString();
            list.add(new Rel(null, to, "DROP"));
        } else if (st instanceof AlterTableStmt) {
            String to = ((AlterTableStmt) st).getTbl().toString();
            list.add(new Rel(null, to, "ALTER"));
        } else if (st instanceof CreateTableAsSelectStmt) {
            String to = ((CreateTableAsSelectStmt) st).getInsertStmt().getTableName().toString();
            for (String from : getQuery(((CreateTableAsSelectStmt) st).getQueryStatement().getQueryRelation())) {
                list.add(new Rel(from, to, "CREATE"));
            }
        } else if (st instanceof EmptyStmt) {
            //不做解析
        } else if (st instanceof SetStmt) {
            //不做解析
        } else if (st instanceof CreateViewStmt) {
            //不做解析
        } else {
            throw new RuntimeException(st.getClass() + "");
        }
        return list;

    }


    private static Set<String> getQuery(Relation r) {
        Set<String> res = new HashSet<>();
        getQuery(r, res, new HashMap<>());
        return res;
    }

    /**
     * @param r   需要分析的relation
     * @param res 将解析出来的查询放入res中
     * @param map map用于解析  WITH  `T-60`  AS  (select  *  from  t1  left  join  t2)    做一个  T-60  和  t1,t2的映射
     */
    private static void getQuery(Relation r, Set<String> res, Map<String, Set<String>> map) {
        if (r == null) {
            return;
        }
        if (r instanceof SelectRelation) {
            List<CTERelation> cteRelations = ((SelectRelation) r).getCteRelations();
            Map<String, Set<String>> m = new HashMap<>();
            for (CTERelation cteRelation : cteRelations) {
                String name = cteRelation.getName();
                HashSet<String> l = new HashSet<>();
                getQuery(cteRelation.getCteQueryStatement().getQueryRelation(), l, map);
                m.put(name, l);
            }
            getQuery(((SelectRelation) r).getRelation(), res, m);
        } else if (r instanceof TableRelation) {
            if (map.containsKey(r.toString())) {
                res.addAll(map.get(r.toString()));
            } else {
                res.add(r.toString());
            }
        } else if (r instanceof SubqueryRelation) {
            getQuery(((SubqueryRelation) r).getQueryStatement().getQueryRelation(), res, map);
        } else if (r instanceof JoinRelation) {
            getQuery(((JoinRelation) r).getLeft(), res, map);
            getQuery(((JoinRelation) r).getRight(), res, map);
        } else if (r instanceof ValuesRelation) {
            //不做解析
        } else {
            throw new RuntimeException(r.getClass() + "");
        }
    }


}
