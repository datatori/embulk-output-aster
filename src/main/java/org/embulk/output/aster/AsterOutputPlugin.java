package org.embulk.output.aster;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.aster.jdbc.AsterBatchInsert;
import org.embulk.output.aster.jdbc.AsterOutputConnector;

public class AsterOutputPlugin
        extends AbstractJdbcOutputPlugin
{

    public interface AsterPluginTask
            extends PluginTask
    {
        @Config("url")
        public String getUrl();

        @Config("user")
        @ConfigDefault("beehive")
        public String getUser();

        @Config("password")
        @ConfigDefault("beehive")
        public String getPassword();

        @Config("database")
        @ConfigDefault("beehive")
        public String getDatabase();

        @Config("table")
        @ConfigDefault("null")
        public String getTable();

        @Config("distribute_key")
        @ConfigDefault("null")
        public Optional<String> getDistributeKey();

        @Config("max_table_name_length")
        @ConfigDefault("30")
        public int getMaxTableNameLength();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return AsterPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        AsterPluginTask t = (AsterPluginTask) task;
        return new Features()
            .setMaxTableNameLength(t.getMaxTableNameLength())
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE));
    }

    @Override
    protected AsterOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        AsterPluginTask t = (AsterPluginTask) task;

        Properties props = new Properties();

        props.putAll(t.getOptions());
        props.setProperty("database", t.getDatabase());
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());

        logger.info("Connecting to {} options {}", t.getUrl(), props);
        props.setProperty("password", t.getPassword());

        return new AsterOutputConnector(t.getUrl(), t.getDistributeKey(), props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new AsterBatchInsert(getConnector(task, true), mergeConfig);
    }
}
