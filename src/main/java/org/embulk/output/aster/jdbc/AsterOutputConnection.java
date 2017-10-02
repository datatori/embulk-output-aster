package org.embulk.output.aster.jdbc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.base.Optional;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.slf4j.Logger;
import org.embulk.spi.Exec;

import static java.lang.String.format;

public class AsterOutputConnection
        extends JdbcOutputConnection
{
    private final Logger logger = Exec.getLogger(AsterOutputConnection.class);
    protected final Connection connection;
    protected final DatabaseMetaData databaseMetaData;
    protected String identifierQuoteString;
    protected Optional<String> schema;
    protected Optional<String> distributeKey;

    public AsterOutputConnection(Connection connection, Optional<String> schema, Optional<String> distributeKey)
            throws SQLException
    {
        super(connection, null);
        this.connection = connection;
        this.databaseMetaData = connection.getMetaData();
        this.schema = schema;
        this.distributeKey = distributeKey;
        this.identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        if (schema.isPresent()) {
            setSearchPath(schema.get());
        }
    }

    @Override
    public void close() throws SQLException
    {
        if (!connection.isClosed()) {
            executeQuery("END;");

            connection.close();
        }
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        return databaseMetaData;
    }

    public Charset getTableNameCharset() throws SQLException
    {
        return StandardCharsets.UTF_8;
    }

    protected void setSearchPath(String schema) throws SQLException
    {
        String sql = "SET search_path TO " + quoteIdentifierString(schema);
        executeQuery(sql);
    }

    @Override
    protected String buildCreateTableIfNotExistsSql(TableIdentifier table, JdbcSchema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ANALYTIC TABLE IF NOT EXISTS ");
        this.quoteTableIdentifier(sb, table);
        sb.append(this.buildCreateTableSchemaSql(schema));
        return sb.toString();
    }

    @Override
    protected void dropTableIfExists(Statement stmt, TableIdentifier table) throws SQLException
    {
        String sql = format("DROP TABLE IF EXISTS %s", this.quoteTableIdentifier(table));
        this.executeUpdate(stmt, sql);
    }

    @Override
    protected String buildCreateTableSql(TableIdentifier table, JdbcSchema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ANALYTIC TABLE ");
        this.quoteTableIdentifier(sb, table);
        sb.append(this.buildCreateTableSchemaSql(schema));
        return sb.toString();
    }


    protected String buildCreateTableSchemaSql(JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(" (");
        for (int i=0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
            sb.append(" ");
            String typeName = getCreateTableTypeName(schema.getColumn(i));
            sb.append(typeName);
        }
        sb.append(")");

        if (distributeKey.isPresent()) {
            sb.append(format("DISTRIBUTE BY HASH(%s)", distributeKey.get()));
        } else {
            sb.append("DISTRIBUTE BY REPLICATION");
        }

        return sb.toString();
    }

    protected String getCreateTableTypeName(JdbcColumn c)
    {
        if (c.getDeclaredType().isPresent()) {
            return c.getDeclaredType().get();
        } else {
            return buildColumnTypeName(c);
        }
    }

    protected String buildColumnTypeName(JdbcColumn c)
    {
        String simpleTypeName = c.getSimpleTypeName();
        switch (getColumnDeclareType(simpleTypeName, c)) {
        case SIZE:
            return format("%s(%d)", simpleTypeName, c.getSizeTypeParameter());
        case SIZE_AND_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return format("%s(%d,0)", simpleTypeName, c.getSizeTypeParameter());
            } else {
                return format("%s(%d,%d)", simpleTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        case SIZE_AND_OPTIONAL_SCALE:
            if (c.getScaleTypeParameter() < 0) {
                return format("%s(%d)", simpleTypeName, c.getSizeTypeParameter());
            } else {
                return format("%s(%d,%d)", simpleTypeName, c.getSizeTypeParameter(), c.getScaleTypeParameter());
            }
        default:  // SIMPLE
            if (simpleTypeName.equals("CLOB"))
            {
                return "CHARACTER VARYING";
            }
            return simpleTypeName;
        }
    }

    protected void executeSql(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    @Override
    protected String buildRenameTableSql(TableIdentifier fromTable, TableIdentifier toTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        this.quoteTableIdentifier(sb, fromTable);
        sb.append(" RENAME TO ");
        this.quoteTableIdentifier(sb, toTable);
        return sb.toString();
    }


    protected void quoteIdentifierString(StringBuilder sb, String str)
    {
        sb.append(quoteIdentifierString(str, identifierQuoteString));
    }

    protected String quoteIdentifierString(String str)
    {
        return quoteIdentifierString(str, identifierQuoteString);
    }

    protected String quoteIdentifierString(String str, String quoteString)
    {
        // TODO if identifierQuoteString.equals(" ") && str.contains([^a-zA-Z0-9_connection.getMetaData().getExtraNameCharacters()])
        // TODO if str.contains(identifierQuoteString);
        return quoteString + str + quoteString;
    }

    public boolean isValidConnection(int timeout) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeQuery("SELECT 1").close();
            return true;
        } catch (SQLException ex) {
            return false;
        } finally {
            stmt.close();
        }
    }

    protected String[] getDeterministicSqlStates()
    {
        return new String[0];
    }

    protected int[] getDeterministicErrorCodes()
    {
        return new int[0];
    }

    protected Class[] getDeterministicRootCauses()
    {
        return new Class[] {
            // Don't retry on UnknownHostException.
            java.net.UnknownHostException.class,

            //// we should not retry on connect() error?
            //java.net.ConnectException.class,
        };
    }

    public boolean isRetryableException(SQLException exception)
    {
        String sqlState = exception.getSQLState();
        for (String deterministic : getDeterministicSqlStates()) {
            if (sqlState.equals(deterministic)) {
                return false;
            }
        }

        int errorCode = exception.getErrorCode();
        for (int deterministic : getDeterministicErrorCodes()) {
            if (errorCode == deterministic) {
                return false;
            }
        }

        Throwable rootCause = getRootCause(exception);
        for (Class deterministic : getDeterministicRootCauses()) {
            if (deterministic.equals(rootCause.getClass())) {
                return false;
            }
        }

        return true;
    }

    private Throwable getRootCause(Throwable e) {
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    protected void executeQuery(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } finally {
            stmt.close();
        }
    }

    protected int executeUpdate(Statement stmt, String sql) throws SQLException
    {
        logger.info("SQL: " + sql);
        long startTime = System.currentTimeMillis();
        int count = stmt.executeUpdate(sql);
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        if (count == 0) {
            logger.info(format("> %.2f seconds", seconds));
        } else {
            logger.info(format("> %.2f seconds (%,d rows)", seconds, count));
        }
        return count;
    }

    protected void commitIfNecessary(Connection con) throws SQLException
    {
        if (!con.getAutoCommit()) {
            con.commit();
        }
    }

    protected SQLException safeRollback(Connection con, SQLException cause)
    {
        try {
            if (!con.getAutoCommit()) {
                con.rollback();
            }
            return cause;
        } catch (SQLException ex) {
            if (cause != null) {
                cause.addSuppressed(ex);
                return cause;
            }
            return ex;
        }
    }
}
