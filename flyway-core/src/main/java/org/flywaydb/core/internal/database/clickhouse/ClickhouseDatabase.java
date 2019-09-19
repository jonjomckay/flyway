/*
 * Copyright 2010-2019 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.clickhouse;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ClickHouse database.
 */
public class ClickhouseDatabase extends Database<ClickhouseConnection> {
    /**
     * Creates a new instance.
     *
     * @param configuration The Flyway configuration.
     */
    public ClickhouseDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory



    ) {
        super(configuration, jdbcConnectionFactory



        );
    }

    @Override
    protected ClickhouseConnection doGetConnection(Connection connection



    ) {
        return new ClickhouseConnection(this, connection);
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        // ClickHouse doesn't appear to have any concept of users
        return "null";
    }

    @Override
    public final void ensureSupported() {
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public boolean supportsChangingCurrentSchema() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public String doQuote(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public boolean catalogIsSchema() {
        return true;
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        return "CREATE TABLE " + table + " (\n" +
                "    installed_rank Int32,\n" +
                "    version Nullable(String),\n" +
                "    description String,\n" +
                "    type String,\n" +
                "    script String,\n" +
                "    checksum Nullable(Int32),\n" +
                "    installed_by Nullable(String),\n" +
                "    installed_on DateTime,\n" +
                "    execution_time Int32,\n" +
                "    success UInt8\n" +
                ") ENGINE = TinyLog;\n" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
    }
}
