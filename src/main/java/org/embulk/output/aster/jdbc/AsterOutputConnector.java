package org.embulk.output.aster.jdbc;

import com.google.common.base.Optional;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.sql.Connection;
import java.sql.SQLException;

public class AsterOutputConnector
        implements JdbcOutputConnector
{
    private final Logger logger = Exec.getLogger(AsterOutputConnector.class);
    private final String url;
    private final Properties properties;
    private final Optional<String> schema;
    private final Optional<String> distributeKey;

    public AsterOutputConnector(String url, Optional<String> schema, Optional<String> distributeKey, Properties properties)
    {
        this.url = url;
        this.schema = schema;
        this.distributeKey = distributeKey;
        this.properties = properties;
    }

    @Override
    public AsterOutputConnection connect(boolean autoCommit) throws SQLException
    {
        try
        {
            Class.forName("com.asterdata.ncluster.Driver");
        } catch (ClassNotFoundException e)
        {
            logger.error("Class not found: " + e.getMessage());
        }

        Connection con = DriverManager.getConnection(url, properties);

        try {
            Statement stmt = con.createStatement();
            String sql = "BEGIN;";
            logger.info("SQL: " + sql);
            stmt.execute(sql);

            AsterOutputConnection c = new AsterOutputConnection(con, schema, distributeKey);
            con = null;
            return c;
        }
        finally {
            if (con != null) {
                con.close();
            }
        }
    }
}