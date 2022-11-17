package org.kendar.janus;

import org.kendar.janus.cmd.Exec;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
import org.kendar.janus.serialization.TypedSerializable;
import org.kendar.janus.serialization.TypedSerializer;

import java.sql.*;

public class JdbcDatabaseMetaData implements DatabaseMetaData, JdbcResult {
    private JdbcConnection connection;
    private Engine engine;
    private long traceId;



    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }

    public JdbcDatabaseMetaData(){

    }

    public JdbcDatabaseMetaData(JdbcConnection jdbcConnection, Engine engine) {
        connection = jdbcConnection;
        this.engine = engine;
    }


    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }


    public void initialize(JdbcConnection jdbcConnection, Engine engine) {
        connection = jdbcConnection;
        this.engine = engine;
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("traceId",traceId);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer builder) {
        traceId = builder.read("traceId");
        return this;
    }

    //TODO Implements
    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "allProceduresAreCallable")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "allTablesAreSelectable")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getURL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getURL")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getUserName() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getUserName")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "isReadOnly")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "nullsAreSortedHigh")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "nullsAreSortedLow")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "nullsAreSortedAtStart")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "nullsAreSortedAtEnd")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDatabaseProductName")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDatabaseProductVersion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getDriverName() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDriverName")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDriverVersion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getDriverMajorVersion() {

        try {
            return ((ObjectResult)engine.execute(new Exec(
                            "getDriverMajorVersion")
                    ,connection.getTraceId(),connection.getTraceId())).getResult();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getDriverMinorVersion() {
        try {
            return ((ObjectResult)engine.execute(new Exec(
                            "getDriverMinorVersion")
                    ,connection.getTraceId(),connection.getTraceId())).getResult();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "usesLocalFiles")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "usesLocalFilePerTable")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsMixedCaseIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "storesUpperCaseIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "storesLowerCaseIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "storesMixedCaseIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsMixedCaseQuotedIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "storesUpperCaseQuotedIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "storesLowerCaseQuotedIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "storesMixedCaseQuotedIdentifiers")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getIdentifierQuoteString")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSQLKeywords")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getNumericFunctions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getStringFunctions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSystemFunctions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getTimeDateFunctions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSearchStringEscape")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getExtraNameCharacters")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsAlterTableWithAddColumn")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsAlterTableWithDropColumn")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsColumnAliasing")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "nullPlusNonNullIsNull")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsConvert")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsTableCorrelationNames")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsDifferentTableCorrelationNames")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsExpressionsInOrderBy")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsOrderByUnrelated")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsGroupBy")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsGroupByUnrelated")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsGroupByBeyondSelect")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsLikeEscapeClause")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsMultipleResultSets")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsMultipleTransactions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsNonNullableColumns")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsMinimumSQLGrammar")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return ((ObjectResult) engine.execute(new Exec(
                        "supportsCoreSQLGrammar")
                , connection.getTraceId(), connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsExtendedSQLGrammar")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsANSI92EntryLevelSQL")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsANSI92IntermediateSQL")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsANSI92FullSQL")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsIntegrityEnhancementFacility")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsOuterJoins")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsFullOuterJoins")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsLimitedOuterJoins")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSchemaTerm")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getProcedureTerm")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getCatalogTerm")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "isCatalogAtStart")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getCatalogSeparator")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSchemasInDataManipulation")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSchemasInProcedureCalls")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSchemasInTableDefinitions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSchemasInIndexDefinitions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSchemasInPrivilegeDefinitions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsCatalogsInDataManipulation")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsCatalogsInProcedureCalls")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsCatalogsInTableDefinitions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsCatalogsInIndexDefinitions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsCatalogsInPrivilegeDefinitions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsPositionedDelete")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsPositionedUpdate")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSelectForUpdate")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsStoredProcedures")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSubqueriesInComparisons")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSubqueriesInExists")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSubqueriesInIns")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSubqueriesInQuantifieds")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsCorrelatedSubqueries")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsUnion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsUnionAll")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsOpenCursorsAcrossCommit")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsOpenCursorsAcrossRollback")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsOpenStatementsAcrossCommit")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsOpenStatementsAcrossRollback")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxBinaryLiteralLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxCharLiteralLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxColumnNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxColumnsInGroupBy")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxColumnsInIndex")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxColumnsInOrderBy")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxColumnsInSelect")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxColumnsInTable")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxConnections")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxCursorNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxIndexLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxSchemaNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxProcedureNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxCatalogNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxRowSize")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "doesMaxRowSizeIncludeBlobs")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxStatementLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxStatements")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxTableNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxTablesInSelect")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getMaxUserNameLength")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDefaultTransactionIsolation")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsTransactions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsDataDefinitionAndDataManipulationTransactions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsDataManipulationTransactionsOnly")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "dataDefinitionCausesTransactionCommit")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "dataDefinitionIgnoredInTransactions")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSchemas")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getCatalogs")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getTableTypes")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getTypeInfo")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsBatchUpdates")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsSavepoints")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsNamedParameters")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsMultipleOpenResults")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsGetGeneratedKeys")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getResultSetHoldability")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDatabaseMajorVersion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getDatabaseMinorVersion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getJDBCMajorVersion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getJDBCMinorVersion")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "getSQLStateType")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "locatorsUpdateCopy")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsStatementPooling")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "supportsStoredFunctionsUsingCallSyntax")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "autoCommitFailureClosesAllResultSets")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(
                        "generatedKeyAlwaysReturned")
                ,connection.getTraceId(),connection.getTraceId())).getResult();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
