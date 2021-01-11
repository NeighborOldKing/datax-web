package com.wugui.datax.admin.tool.query;

import com.wugui.datax.admin.core.util.LocalCacheUtil;
import com.wugui.datax.admin.entity.JobDatasource;
import com.wugui.datax.admin.tool.meta.DatabaseInterface;
import com.wugui.datax.admin.tool.meta.DatabaseMetaFactory;
import com.wugui.datax.admin.tool.meta.OracleDatabaseMeta;
import com.wugui.datax.admin.util.JdbcUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Oracle数据库使用的查询工具
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MySQLQueryTool
 * @Version 1.0
 * @since 2019/7/18 9:31
 */
public class OracleQueryTool extends BaseQueryTool implements QueryToolInterface {

    //    public OracleQueryTool(JobDatasource jobDatasource) throws SQLException {
//        super(jobDatasource);
//    }
    private Connection connection;
    private OracleDatabaseMeta oracleDatabaseMeta;
    private DatabaseInterface sqlBuilder;
    // 注意static
    private static String tableSchema;

    public OracleQueryTool(JobDatasource jobDatasource) throws SQLException {
        super(jobDatasource);
        this.connection = (Connection) LocalCacheUtil.get(jobDatasource.getDatasourceName());
        sqlBuilder = DatabaseMetaFactory.getByDbType(jobDatasource.getDatasource());
        oracleDatabaseMeta = OracleDatabaseMeta.getInstance();
    }

//    @Override
//    public List getTableSchema() {
//        List schemas = new ArrayList<>();
//        Statement stmt = null;
//        ResultSet rs = null;
//        try {
//            stmt = connection.createStatement();
//            //获取sql
//            String sql = oracleDatabaseMeta.getSQLQueryOwners();
//            rs = stmt.executeQuery(sql);
//            while (rs.next()) {
//                String tableName = rs.getString(1);
//                schemas.add(tableName);
//            }
//        } catch (SQLException e) {
//            logger.error("[getTableNames Exception] --> the exception message is:" + e.getMessage());
//        } finally {
//            JdbcUtils.close(rs);
//            JdbcUtils.close(stmt);
//        }
//        return schemas;
//    }

    @Override
    public List getTableNames(String tableSchema) {
        this.tableSchema = tableSchema;
        List tableOrViews = new ArrayList();
        //获取table的sql
        String tableSql = getSQLQueryTables(tableSchema);
        String viewSql = oracleDatabaseMeta.getSQLQueryViews(tableSchema);
        getTableNamesOrViewsBySql(tableSql, tableOrViews); // 添加表
        getTableNamesOrViewsBySql(viewSql, tableOrViews); // 添加视图
        return tableOrViews;
    }

    @Override
    public List getColumnNames(String tableName, String datasource) {
        String oarcleQueryTableName = tableSchema + "." + tableName;
        return super.getColumnNames(oarcleQueryTableName, datasource);
    }

    private Boolean getTableNamesOrViewsBySql(String sql, List tables) {
        Boolean getTableNamesOrViewsSuccess;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
            tables.sort(Comparator.naturalOrder());
            getTableNamesOrViewsSuccess = true;
        } catch (SQLException e) {
            getTableNamesOrViewsSuccess = false;
            logger.error("[getTableNamesBySql(String sql) Exception] --> the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return getTableNamesOrViewsSuccess;
    }
}
