package org.kendar.janus;

import org.kendar.janus.cmd.Exec;
import org.kendar.janus.cmd.Exec;
import org.kendar.janus.engine.Engine;
import org.kendar.janus.results.JdbcResult;
import org.kendar.janus.results.ObjectResult;
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

    
    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "allProceduresAreCallable")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "allTablesAreSelectable")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getURL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getURL")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getUserName() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getUserName")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "isReadOnly")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "nullsAreSortedHigh")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "nullsAreSortedLow")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "nullsAreSortedAtStart")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "nullsAreSortedAtEnd")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDatabaseProductName")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDatabaseProductVersion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getDriverName() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDriverName")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDriverVersion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getDriverMajorVersion() {

        try {
            return ((ObjectResult)engine.execute(new Exec(this,
                            "getDriverMajorVersion")
                    ,connection.getTraceId(),getTraceId())).getResult();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getDriverMinorVersion() {
        try {
            return ((ObjectResult)engine.execute(new Exec(this,
                            "getDriverMinorVersion")
                    ,connection.getTraceId(),getTraceId())).getResult();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "usesLocalFiles")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "usesLocalFilePerTable")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsMixedCaseIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "storesUpperCaseIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "storesLowerCaseIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "storesMixedCaseIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsMixedCaseQuotedIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "storesUpperCaseQuotedIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "storesLowerCaseQuotedIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "storesMixedCaseQuotedIdentifiers")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getIdentifierQuoteString")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getSQLKeywords")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getNumericFunctions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getStringFunctions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getSystemFunctions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getTimeDateFunctions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getSearchStringEscape")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getExtraNameCharacters")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsAlterTableWithAddColumn")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsAlterTableWithDropColumn")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsColumnAliasing")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "nullPlusNonNullIsNull")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsConvert")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsConvert")
                        .withTypes(int.class,int.class)
                        .withParameters(fromType,toType)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsTableCorrelationNames")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsDifferentTableCorrelationNames")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsExpressionsInOrderBy")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsOrderByUnrelated")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsGroupBy")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsGroupByUnrelated")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsGroupByBeyondSelect")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsLikeEscapeClause")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsMultipleResultSets")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsMultipleTransactions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsNonNullableColumns")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsMinimumSQLGrammar")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return ((ObjectResult) engine.execute(new Exec(this,
                        "supportsCoreSQLGrammar")
                , connection.getTraceId(), getTraceId())).getResult();
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsExtendedSQLGrammar")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsANSI92EntryLevelSQL")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsANSI92IntermediateSQL")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsANSI92FullSQL")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsIntegrityEnhancementFacility")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsOuterJoins")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsFullOuterJoins")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsLimitedOuterJoins")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getSchemaTerm")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getProcedureTerm")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getCatalogTerm")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "isCatalogAtStart")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getCatalogSeparator")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSchemasInDataManipulation")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSchemasInProcedureCalls")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSchemasInTableDefinitions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSchemasInIndexDefinitions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSchemasInPrivilegeDefinitions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsCatalogsInDataManipulation")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsCatalogsInProcedureCalls")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsCatalogsInTableDefinitions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsCatalogsInIndexDefinitions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsCatalogsInPrivilegeDefinitions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsPositionedDelete")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsPositionedUpdate")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSelectForUpdate")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsStoredProcedures")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSubqueriesInComparisons")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSubqueriesInExists")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSubqueriesInIns")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSubqueriesInQuantifieds")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsCorrelatedSubqueries")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsUnion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsUnionAll")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsOpenCursorsAcrossCommit")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsOpenCursorsAcrossRollback")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsOpenStatementsAcrossCommit")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsOpenStatementsAcrossRollback")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxBinaryLiteralLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxCharLiteralLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxColumnNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxColumnsInGroupBy")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxColumnsInIndex")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxColumnsInOrderBy")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxColumnsInSelect")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxColumnsInTable")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxConnections")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxCursorNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxIndexLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxSchemaNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxProcedureNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxCatalogNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxRowSize")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "doesMaxRowSizeIncludeBlobs")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxStatementLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxStatements")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxTableNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxTablesInSelect")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getMaxUserNameLength")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDefaultTransactionIsolation")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsTransactions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsTransactionIsolationLevel")
                        .withTypes(int.class)
                        .withParameters(level)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsDataDefinitionAndDataManipulationTransactions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsDataManipulationTransactionsOnly")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "dataDefinitionCausesTransactionCommit")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "dataDefinitionIgnoredInTransactions")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getProcedures")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,procedureNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getProcedureColumns")
                        .withTypes(String.class,String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,procedureNamePattern,columnNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getTables")
                        .withTypes(String.class,String.class,String.class,String[].class)
                        .withParameters(catalog,schemaPattern,tableNamePattern,types)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getSchemas")
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getCatalogs")
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getTableTypes")
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getColumns")
                        .withTypes(String.class,String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,tableNamePattern,columnNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getColumnPrivileges")
                        .withTypes(String.class,String.class,String.class,String.class)
                        .withParameters(catalog,schema,table,columnNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getTablePrivileges")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,tableNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getBestRowIdentifier")
                        .withTypes(String.class,String.class,String.class,int.class,boolean.class)
                        .withParameters(catalog,schema,table,scope,nullable)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getVersionColumns")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schema,table)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getPrimaryKeys")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schema,table)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getImportedKeys")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schema,table)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getExportedKeys")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schema,table)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getCrossReference")
                        .withTypes(String.class,String.class,String.class,
                                String.class,String.class,String.class)
                        .withParameters(parentCatalog,parentSchema,parentTable,
                                foreignCatalog,foreignSchema,foreignTable)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getTypeInfo")
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getIndexInfo")
                        .withTypes(String.class,String.class,String.class,boolean.class,boolean.class)
                        .withParameters(catalog,schema,table,unique,approximate)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsResultSetType")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsResultSetConcurrency")
                        .withTypes(int.class,int.class)
                        .withParameters(type,concurrency)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "ownUpdatesAreVisible")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "ownDeletesAreVisible")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "ownInsertsAreVisible")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "othersUpdatesAreVisible")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "othersDeletesAreVisible")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "othersInsertsAreVisible")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "updatesAreDetected")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "deletesAreDetected")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "insertsAreDetected")
                        .withTypes(int.class)
                        .withParameters(type)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsBatchUpdates")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getUDTs")
                        .withTypes(String.class,String.class,String.class,int[].class)
                        .withParameters(catalog,schemaPattern,typeNamePattern,types)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsSavepoints")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsNamedParameters")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsMultipleOpenResults")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsGetGeneratedKeys")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getSuperTypes")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,typeNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getSuperTables")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,tableNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getAttributes")
                        .withTypes(String.class,String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,typeNamePattern,attributeNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsResultSetHoldability")
                        .withTypes(int.class)
                        .withParameters(holdability)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getResultSetHoldability")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDatabaseMajorVersion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getDatabaseMinorVersion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getJDBCMajorVersion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getJDBCMinorVersion")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getSQLStateType")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "locatorsUpdateCopy")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsStatementPooling")
                ,connection.getTraceId(),getTraceId())).getResult();
    }


    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getSchemas")
                        .withTypes(String.class,String.class)
                        .withParameters(catalog,schemaPattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsStoredFunctionsUsingCallSyntax")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "autoCommitFailureClosesAllResultSets")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getClientInfoProperties")
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getFunctions")
                        .withTypes(String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,functionNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getFunctionColumns")
                        .withTypes(String.class,String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,functionNamePattern,columnNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        var result = (JdbcResultSet)engine.execute(new Exec(this,
                        "getPseudoColumns")
                        .withTypes(String.class,String.class,String.class,String.class)
                        .withParameters(catalog,schemaPattern,tableNamePattern,columnNamePattern)
                ,connection.getTraceId(),getTraceId());
        result.setEngine(engine);
        result.setConnection(connection);
        return result;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "generatedKeyAlwaysReturned")
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(JdbcDatabaseMetaData.class);
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "getRowIdLifetime")
                ,connection.getTraceId(),getTraceId())).getResult();
    }
}
