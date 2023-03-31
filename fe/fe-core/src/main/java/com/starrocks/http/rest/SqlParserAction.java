// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.http.rest;

import com.google.gson.Gson;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.EmptyStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.ShowCollationStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowLoadStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.parser.SqlParser;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description TODO
 * @Auther tao.zhou
 * @Date 2023-3-22   19:14
 * @Version 1.0
 */
public class SqlParserAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(SqlParserAction.class);

    private static final String PARAM = "sql";

    public SqlParserAction(ActionController controller) {
        super(controller);
    }

    static Set<Class<? extends StatementBase>> statementSkip = new HashSet<>();
    static Set<Class<? extends Relation>> relationSkip = new HashSet<>();

    static {
        statementSkip.add(EmptyStmt.class);
        statementSkip.add(SetStmt.class);
        statementSkip.add(CreateViewStmt.class);
        statementSkip.add(ShowTableStmt.class);
        statementSkip.add(ShowWarningStmt.class);
        statementSkip.add(UseDbStmt.class);
        statementSkip.add(ShowDataStmt.class);
        statementSkip.add(ShowVariablesStmt.class);
        statementSkip.add(ShowCollationStmt.class);
        statementSkip.add(ShowLoadStmt.class);
        relationSkip.add(ValuesRelation.class);
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
        result.addResultEntry("error", "sql   param   is   null,   please   set   valid   param!");
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
        } else if (st != null && !statementSkip.contains(st.getClass())) {
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
     * @param map map用于解析   WITH   `T-60`   AS   (select   *   from   t1   left   join   t2)      做一个   T-60   和   t1,t2的映射
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
            // 解析item中可能的function
            for (SelectListItem item : ((SelectRelation) r).getSelectList().getItems()) {
                expr(item.getExpr(), res, map);
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
        } else if (r instanceof UnionRelation) {
            for (QueryRelation un : ((UnionRelation) r).getRelations()) {
                getQuery(un, res, map);
            }
        } else if (!relationSkip.contains(r.getClass())) {
            throw new RuntimeException(r.getClass() + "");
        }
    }

    private static void expr(Expr expr, Set<String> res, Map<String, Set<String>> map) {
        if (expr instanceof Subquery) {
            getQuery(((Subquery) expr).getQueryStatement().getQueryRelation(), res, map);
        } else if (expr instanceof FunctionCallExpr) {
            for (Expr child : expr.getChildren()) {
                expr(child, res, map);
            }
        }
    }


}


