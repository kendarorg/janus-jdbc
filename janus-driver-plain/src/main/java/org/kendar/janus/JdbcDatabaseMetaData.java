package org.kendar.janus;

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

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    public JdbcDatabaseMetaData(){

    }

    public JdbcDatabaseMetaData(JdbcConnection jdbcConnection, Engine engine) {
        connection = jdbcConnection;
        this.engine = engine;
    }
    private boolean isReadOnly;
    private String getURL;
    private boolean autoCommitFailureClosesAllResultSets;
    private boolean supportsStoredFunctionsUsingCallSyntax;
    private boolean supportsSchemasInPrivilegeDefinitions;
    private boolean supportsIntegrityEnhancementFacility;
    private boolean supportsDifferentTableCorrelationNames;
    private boolean supportsOpenStatementsAcrossRollback;
    private boolean supportsDataManipulationTransactionsOnly;
    private boolean supportsCatalogsInPrivilegeDefinitions;
    private boolean dataDefinitionCausesTransactionCommit;
    private boolean dataDefinitionIgnoredInTransactions;
    private boolean supportsDataDefinitionAndDataManipulationTransactions;
    private boolean supportsANSI92EntryLevelSQL;
    private boolean supportsSubqueriesInComparisons;
    private boolean supportsMultipleResultSets;
    private boolean supportsExtendedSQLGrammar;
    private boolean supportsSchemasInTableDefinitions;
    private boolean supportsOrderByUnrelated;
    private boolean supportsNonNullableColumns;
    private boolean supportsCoreSQLGrammar;
    private boolean supportsCatalogsInProcedureCalls;
    private boolean supportsSelectForUpdate;
    private boolean supportsMultipleTransactions;
    private boolean supportsOpenCursorsAcrossCommit;
    private int getMaxColumnNameLength;
    private int getMaxColumnsInIndex;
    private int getMaxColumnsInSelect;
    private int getMaxSchemaNameLength;
    private int getMaxBinaryLiteralLength;
    private int getMaxProcedureNameLength;
    private int getMaxCatalogNameLength;
    private boolean supportsSchemasInIndexDefinitions;
    private String getCatalogSeparator;
    private boolean supportsSubqueriesInQuantifieds;
    private boolean supportsPositionedDelete;
    private boolean supportsGroupByUnrelated;
    private boolean doesMaxRowSizeIncludeBlobs;
    private int getMaxStatementLength;
    private int getMaxColumnsInGroupBy;
    private int getMaxUserNameLength;
    private boolean supportsSchemasInProcedureCalls;
    private boolean supportsCatalogsInTableDefinitions;
    private boolean supportsPositionedUpdate;
    private int getDefaultTransactionIsolation;
    private int getMaxColumnsInOrderBy;
    private boolean supportsANSI92IntermediateSQL;
    private int getMaxTableNameLength;
    private boolean supportsCorrelatedSubqueries;
    private boolean supportsANSI92FullSQL;
    private boolean supportsCatalogsInDataManipulation;
    private boolean supportsCatalogsInIndexDefinitions;
    private boolean supportsFullOuterJoins;
    private boolean supportsGroupByBeyondSelect;
    private boolean supportsStoredProcedures;
    private boolean supportsOpenCursorsAcrossRollback;
    private boolean supportsOpenStatementsAcrossCommit;
    private int getMaxColumnsInTable;
    private boolean supportsLikeEscapeClause;
    private boolean supportsMinimumSQLGrammar;
    private boolean supportsSchemasInDataManipulation;
    private boolean supportsSubqueriesInIns;
    private int getMaxCursorNameLength;
    private int getMaxTablesInSelect;
    private boolean supportsTransactions;
    private boolean supportsLimitedOuterJoins;
    private int getMaxCharLiteralLength;
    private boolean supportsSubqueriesInExists;
    private boolean supportsMixedCaseQuotedIdentifiers;
    private String getDatabaseProductName;
    private int getDriverMinorVersion;
    private boolean supportsAlterTableWithDropColumn;
    private boolean nullsAreSortedAtStart;
    private boolean nullPlusNonNullIsNull;
    private String getSearchStringEscape;
    private boolean supportsExpressionsInOrderBy;
    private String getDatabaseProductVersion;
    private String getNumericFunctions;
    private boolean storesUpperCaseIdentifiers;
    private boolean usesLocalFilePerTable;
    private boolean nullsAreSortedAtEnd;
    private boolean supportsMixedCaseIdentifiers;
    private boolean storesLowerCaseIdentifiers;
    private boolean storesMixedCaseQuotedIdentifiers;
    private boolean allProceduresAreCallable;
    private boolean storesMixedCaseIdentifiers;
    private boolean allTablesAreSelectable;
    private boolean storesLowerCaseQuotedIdentifiers;
    private String getIdentifierQuoteString;
    private String getTimeDateFunctions;
    private boolean supportsAlterTableWithAddColumn;
    private int getDriverMajorVersion;
    private String getExtraNameCharacters;
    private boolean supportsColumnAliasing;
    private boolean supportsTableCorrelationNames;
    private boolean storesUpperCaseQuotedIdentifiers;
    private boolean generatedKeyAlwaysReturned;
    private int getJDBCMajorVersion;
    private int getResultSetHoldability;
    private long getMaxLogicalLobSize;
    private boolean supportsMultipleOpenResults;
    private int getDatabaseMajorVersion;
    private int getDatabaseMinorVersion;
    private boolean supportsStatementPooling;
    private boolean supportsBatchUpdates;
    private boolean supportsGetGeneratedKeys;
    private boolean supportsNamedParameters;
    private int getJDBCMinorVersion;
    private String getUserName;
    private boolean nullsAreSortedLow;
    private String getDriverName;
    private boolean supportsConvert;
    private int getMaxStatements;
    private boolean supportsUnion;
    private boolean supportsOuterJoins;
    private String getSQLKeywords;
    private boolean supportsUnionAll;
    private String getDriverVersion;
    private int getMaxConnections;
    private boolean isCatalogAtStart;
    private boolean nullsAreSortedHigh;
    private String getCatalogTerm;
    private int getSQLStateType;
    private boolean locatorsUpdateCopy;
    private boolean usesLocalFiles;
    private boolean supportsGroupBy;
    private int getMaxIndexLength;
    private String getProcedureTerm;
    private int getMaxRowSize;
    private String getSchemaTerm;
    private boolean supportsSavepoints;
    private String getSystemFunctions;
    private String getStringFunctions;
    private boolean supportsSharding;
    private boolean supportsRefCursors;


    @Override
    public boolean isReadOnly(){ return isReadOnly;}

    @Override
    public String getURL(){ return getURL;}


    @Override
    public boolean autoCommitFailureClosesAllResultSets(){ return autoCommitFailureClosesAllResultSets;}

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax(){ return supportsStoredFunctionsUsingCallSyntax;}

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions(){ return supportsSchemasInPrivilegeDefinitions;}

    @Override
    public boolean supportsIntegrityEnhancementFacility(){ return supportsIntegrityEnhancementFacility;}

    @Override
    public boolean supportsDifferentTableCorrelationNames(){ return supportsDifferentTableCorrelationNames;}

    @Override
    public boolean supportsOpenStatementsAcrossRollback(){ return supportsOpenStatementsAcrossRollback;}

    @Override
    public boolean supportsDataManipulationTransactionsOnly(){ return supportsDataManipulationTransactionsOnly;}

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions(){ return supportsCatalogsInPrivilegeDefinitions;}

    @Override
    public boolean dataDefinitionCausesTransactionCommit(){ return dataDefinitionCausesTransactionCommit;}

    @Override
    public boolean dataDefinitionIgnoredInTransactions(){ return dataDefinitionIgnoredInTransactions;}

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions(){ return supportsDataDefinitionAndDataManipulationTransactions;}

    @Override
    public boolean supportsANSI92EntryLevelSQL(){ return supportsANSI92EntryLevelSQL;}

    @Override
    public boolean supportsSubqueriesInComparisons(){ return supportsSubqueriesInComparisons;}

    @Override
    public boolean supportsMultipleResultSets(){ return supportsMultipleResultSets;}

    @Override
    public boolean supportsExtendedSQLGrammar(){ return supportsExtendedSQLGrammar;}

    @Override
    public boolean supportsSchemasInTableDefinitions(){ return supportsSchemasInTableDefinitions;}

    @Override
    public boolean supportsOrderByUnrelated(){ return supportsOrderByUnrelated;}

    @Override
    public boolean supportsNonNullableColumns(){ return supportsNonNullableColumns;}

    @Override
    public boolean supportsCoreSQLGrammar(){ return supportsCoreSQLGrammar;}

    @Override
    public boolean supportsCatalogsInProcedureCalls(){ return supportsCatalogsInProcedureCalls;}

    @Override
    public boolean supportsSelectForUpdate(){ return supportsSelectForUpdate;}

    @Override
    public boolean supportsMultipleTransactions(){ return supportsMultipleTransactions;}

    @Override
    public boolean supportsOpenCursorsAcrossCommit(){ return supportsOpenCursorsAcrossCommit;}

    @Override
    public int getMaxColumnNameLength(){ return getMaxColumnNameLength;}

    @Override
    public int getMaxColumnsInIndex(){ return getMaxColumnsInIndex;}

    @Override
    public int getMaxColumnsInSelect(){ return getMaxColumnsInSelect;}

    @Override
    public int getMaxSchemaNameLength(){ return getMaxSchemaNameLength;}

    @Override
    public int getMaxBinaryLiteralLength(){ return getMaxBinaryLiteralLength;}

    @Override
    public int getMaxProcedureNameLength(){ return getMaxProcedureNameLength;}

    @Override
    public int getMaxCatalogNameLength(){ return getMaxCatalogNameLength;}

    @Override
    public boolean supportsSchemasInIndexDefinitions(){ return supportsSchemasInIndexDefinitions;}

    @Override
    public String getCatalogSeparator(){ return getCatalogSeparator;}

    @Override
    public boolean supportsSubqueriesInQuantifieds(){ return supportsSubqueriesInQuantifieds;}

    @Override
    public boolean supportsPositionedDelete(){ return supportsPositionedDelete;}

    @Override
    public boolean supportsGroupByUnrelated(){ return supportsGroupByUnrelated;}

    @Override
    public boolean doesMaxRowSizeIncludeBlobs(){ return doesMaxRowSizeIncludeBlobs;}

    @Override
    public int getMaxStatementLength(){ return getMaxStatementLength;}

    @Override
    public int getMaxColumnsInGroupBy(){ return getMaxColumnsInGroupBy;}

    @Override
    public int getMaxUserNameLength(){ return getMaxUserNameLength;}

    @Override
    public boolean supportsSchemasInProcedureCalls(){ return supportsSchemasInProcedureCalls;}

    @Override
    public boolean supportsCatalogsInTableDefinitions(){ return supportsCatalogsInTableDefinitions;}

    @Override
    public boolean supportsPositionedUpdate(){ return supportsPositionedUpdate;}

    @Override
    public int getDefaultTransactionIsolation(){ return getDefaultTransactionIsolation;}

    @Override
    public int getMaxColumnsInOrderBy(){ return getMaxColumnsInOrderBy;}

    @Override
    public boolean supportsANSI92IntermediateSQL(){ return supportsANSI92IntermediateSQL;}

    @Override
    public int getMaxTableNameLength(){ return getMaxTableNameLength;}

    @Override
    public boolean supportsCorrelatedSubqueries(){ return supportsCorrelatedSubqueries;}

    @Override
    public boolean supportsANSI92FullSQL(){ return supportsANSI92FullSQL;}

    @Override
    public boolean supportsCatalogsInDataManipulation(){ return supportsCatalogsInDataManipulation;}

    @Override
    public boolean supportsCatalogsInIndexDefinitions(){ return supportsCatalogsInIndexDefinitions;}

    @Override
    public boolean supportsFullOuterJoins(){ return supportsFullOuterJoins;}

    @Override
    public boolean supportsGroupByBeyondSelect(){ return supportsGroupByBeyondSelect;}

    @Override
    public boolean supportsStoredProcedures(){ return supportsStoredProcedures;}

    @Override
    public boolean supportsOpenCursorsAcrossRollback(){ return supportsOpenCursorsAcrossRollback;}

    @Override
    public boolean supportsOpenStatementsAcrossCommit(){ return supportsOpenStatementsAcrossCommit;}

    @Override
    public int getMaxColumnsInTable(){ return getMaxColumnsInTable;}

    @Override
    public boolean supportsLikeEscapeClause(){ return supportsLikeEscapeClause;}

    @Override
    public boolean supportsMinimumSQLGrammar(){ return supportsMinimumSQLGrammar;}

    @Override
    public boolean supportsSchemasInDataManipulation(){ return supportsSchemasInDataManipulation;}

    @Override
    public boolean supportsSubqueriesInIns(){ return supportsSubqueriesInIns;}

    @Override
    public int getMaxCursorNameLength(){ return getMaxCursorNameLength;}

    @Override
    public int getMaxTablesInSelect(){ return getMaxTablesInSelect;}

    @Override
    public boolean supportsTransactions(){ return supportsTransactions;}

    @Override
    public boolean supportsLimitedOuterJoins(){ return supportsLimitedOuterJoins;}

    @Override
    public int getMaxCharLiteralLength(){ return getMaxCharLiteralLength;}

    @Override
    public boolean supportsSubqueriesInExists(){ return supportsSubqueriesInExists;}

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers(){ return supportsMixedCaseQuotedIdentifiers;}

    @Override
    public String getDatabaseProductName(){ return getDatabaseProductName;}

    @Override
    public int getDriverMinorVersion(){ return getDriverMinorVersion;}

    @Override
    public boolean supportsAlterTableWithDropColumn(){ return supportsAlterTableWithDropColumn;}

    @Override
    public boolean nullsAreSortedAtStart(){ return nullsAreSortedAtStart;}

    @Override
    public boolean nullPlusNonNullIsNull(){ return nullPlusNonNullIsNull;}

    @Override
    public String getSearchStringEscape(){ return getSearchStringEscape;}

    @Override
    public boolean supportsExpressionsInOrderBy(){ return supportsExpressionsInOrderBy;}

    @Override
    public String getDatabaseProductVersion(){ return getDatabaseProductVersion;}

    @Override
    public String getNumericFunctions(){ return getNumericFunctions;}

    @Override
    public boolean storesUpperCaseIdentifiers(){ return storesUpperCaseIdentifiers;}

    @Override
    public boolean usesLocalFilePerTable(){ return usesLocalFilePerTable;}

    @Override
    public boolean nullsAreSortedAtEnd(){ return nullsAreSortedAtEnd;}

    @Override
    public boolean supportsMixedCaseIdentifiers(){ return supportsMixedCaseIdentifiers;}

    @Override
    public boolean storesLowerCaseIdentifiers(){ return storesLowerCaseIdentifiers;}

    @Override
    public boolean storesMixedCaseQuotedIdentifiers(){ return storesMixedCaseQuotedIdentifiers;}

    @Override
    public boolean allProceduresAreCallable(){ return allProceduresAreCallable;}

    @Override
    public boolean storesMixedCaseIdentifiers(){ return storesMixedCaseIdentifiers;}

    @Override
    public boolean allTablesAreSelectable(){ return allTablesAreSelectable;}

    @Override
    public boolean storesLowerCaseQuotedIdentifiers(){ return storesLowerCaseQuotedIdentifiers;}

    @Override
    public String getIdentifierQuoteString(){ return getIdentifierQuoteString;}

    @Override
    public String getTimeDateFunctions(){ return getTimeDateFunctions;}

    @Override
    public boolean supportsAlterTableWithAddColumn(){ return supportsAlterTableWithAddColumn;}

    @Override
    public int getDriverMajorVersion(){ return getDriverMajorVersion;}

    @Override
    public String getExtraNameCharacters(){ return getExtraNameCharacters;}

    @Override
    public boolean supportsColumnAliasing(){ return supportsColumnAliasing;}

    @Override
    public boolean supportsTableCorrelationNames(){ return supportsTableCorrelationNames;}

    @Override
    public boolean storesUpperCaseQuotedIdentifiers(){ return storesUpperCaseQuotedIdentifiers;}

    @Override
    public boolean generatedKeyAlwaysReturned(){ return generatedKeyAlwaysReturned;}


    @Override
    public int getJDBCMajorVersion(){ return getJDBCMajorVersion;}

    @Override
    public int getResultSetHoldability(){ return getResultSetHoldability;}

    @Override
    public long getMaxLogicalLobSize(){ return getMaxLogicalLobSize;}

    @Override
    public boolean supportsMultipleOpenResults(){ return supportsMultipleOpenResults;}

    @Override
    public int getDatabaseMajorVersion(){ return getDatabaseMajorVersion;}

    @Override
    public int getDatabaseMinorVersion(){ return getDatabaseMinorVersion;}

    @Override
    public boolean supportsStatementPooling(){ return supportsStatementPooling;}

    @Override
    public boolean supportsBatchUpdates(){ return supportsBatchUpdates;}

    @Override
    public boolean supportsGetGeneratedKeys(){ return supportsGetGeneratedKeys;}

    @Override
    public boolean supportsNamedParameters(){ return supportsNamedParameters;}

    @Override
    public int getJDBCMinorVersion(){ return getJDBCMinorVersion;}

    @Override
    public String getUserName(){ return getUserName;}

    @Override
    public boolean nullsAreSortedLow(){ return nullsAreSortedLow;}

    @Override
    public String getDriverName(){ return getDriverName;}

    @Override
    public boolean supportsConvert(){ return supportsConvert;}

    @Override
    public int getMaxStatements(){ return getMaxStatements;}

    @Override
    public boolean supportsUnion(){ return supportsUnion;}

    @Override
    public boolean supportsOuterJoins(){ return supportsOuterJoins;}

    @Override
    public String getSQLKeywords(){ return getSQLKeywords;}

    @Override
    public boolean supportsUnionAll(){ return supportsUnionAll;}

    @Override
    public String getDriverVersion(){ return getDriverVersion;}

    @Override
    public int getMaxConnections(){ return getMaxConnections;}

    @Override
    public boolean isCatalogAtStart(){ return isCatalogAtStart;}

    @Override
    public boolean nullsAreSortedHigh(){ return nullsAreSortedHigh;}

    @Override
    public String getCatalogTerm(){ return getCatalogTerm;}

    @Override
    public int getSQLStateType(){ return getSQLStateType;}

    @Override
    public boolean locatorsUpdateCopy(){ return locatorsUpdateCopy;}



    @Override
    public boolean usesLocalFiles(){ return usesLocalFiles;}

    @Override
    public boolean supportsGroupBy(){ return supportsGroupBy;}

    @Override
    public int getMaxIndexLength(){ return getMaxIndexLength;}

    @Override
    public String getProcedureTerm(){ return getProcedureTerm;}

    @Override
    public int getMaxRowSize(){ return getMaxRowSize;}

    @Override
    public String getSchemaTerm(){ return getSchemaTerm;}

    @Override
    public boolean supportsSavepoints(){ return supportsSavepoints;}

    @Override
    public String getSystemFunctions(){ return getSystemFunctions;}

    @Override
    public String getStringFunctions(){ return getStringFunctions;}

    @Override
    public boolean supportsSharding(){ return supportsSharding;}

    @Override
    public boolean supportsRefCursors(){ return supportsRefCursors;}

    public void fill(DatabaseMetaData src) throws SQLException {
        isReadOnly=src.isReadOnly();
        getURL=src.getURL();
        autoCommitFailureClosesAllResultSets=src.autoCommitFailureClosesAllResultSets();
        supportsStoredFunctionsUsingCallSyntax=src.supportsStoredFunctionsUsingCallSyntax();
        supportsSchemasInPrivilegeDefinitions=src.supportsSchemasInPrivilegeDefinitions();
        supportsIntegrityEnhancementFacility=src.supportsIntegrityEnhancementFacility();
        supportsDifferentTableCorrelationNames=src.supportsDifferentTableCorrelationNames();
        supportsOpenStatementsAcrossRollback=src.supportsOpenStatementsAcrossRollback();
        supportsDataManipulationTransactionsOnly=src.supportsDataManipulationTransactionsOnly();
        supportsCatalogsInPrivilegeDefinitions=src.supportsCatalogsInPrivilegeDefinitions();
        dataDefinitionCausesTransactionCommit=src.dataDefinitionCausesTransactionCommit();
        dataDefinitionIgnoredInTransactions=src.dataDefinitionIgnoredInTransactions();
        supportsDataDefinitionAndDataManipulationTransactions=src.supportsDataDefinitionAndDataManipulationTransactions();
        supportsANSI92EntryLevelSQL=src.supportsANSI92EntryLevelSQL();
        supportsSubqueriesInComparisons=src.supportsSubqueriesInComparisons();
        supportsMultipleResultSets=src.supportsMultipleResultSets();
        supportsExtendedSQLGrammar=src.supportsExtendedSQLGrammar();
        supportsSchemasInTableDefinitions=src.supportsSchemasInTableDefinitions();
        supportsOrderByUnrelated=src.supportsOrderByUnrelated();
        supportsNonNullableColumns=src.supportsNonNullableColumns();
        supportsCoreSQLGrammar=src.supportsCoreSQLGrammar();
        supportsCatalogsInProcedureCalls=src.supportsCatalogsInProcedureCalls();
        supportsSelectForUpdate=src.supportsSelectForUpdate();
        supportsMultipleTransactions=src.supportsMultipleTransactions();
        supportsOpenCursorsAcrossCommit=src.supportsOpenCursorsAcrossCommit();
        getMaxColumnNameLength=src.getMaxColumnNameLength();
        getMaxColumnsInIndex=src.getMaxColumnsInIndex();
        getMaxColumnsInSelect=src.getMaxColumnsInSelect();
        getMaxSchemaNameLength=src.getMaxSchemaNameLength();
        getMaxBinaryLiteralLength=src.getMaxBinaryLiteralLength();
        getMaxProcedureNameLength=src.getMaxProcedureNameLength();
        getMaxCatalogNameLength=src.getMaxCatalogNameLength();
        supportsSchemasInIndexDefinitions=src.supportsSchemasInIndexDefinitions();
        getCatalogSeparator=src.getCatalogSeparator();
        supportsSubqueriesInQuantifieds=src.supportsSubqueriesInQuantifieds();
        supportsPositionedDelete=src.supportsPositionedDelete();
        supportsGroupByUnrelated=src.supportsGroupByUnrelated();
        doesMaxRowSizeIncludeBlobs=src.doesMaxRowSizeIncludeBlobs();
        getMaxStatementLength=src.getMaxStatementLength();
        getMaxColumnsInGroupBy=src.getMaxColumnsInGroupBy();
        getMaxUserNameLength=src.getMaxUserNameLength();
        supportsSchemasInProcedureCalls=src.supportsSchemasInProcedureCalls();
        supportsCatalogsInTableDefinitions=src.supportsCatalogsInTableDefinitions();
        supportsPositionedUpdate=src.supportsPositionedUpdate();
        getDefaultTransactionIsolation=src.getDefaultTransactionIsolation();
        getMaxColumnsInOrderBy=src.getMaxColumnsInOrderBy();
        supportsANSI92IntermediateSQL=src.supportsANSI92IntermediateSQL();
        getMaxTableNameLength=src.getMaxTableNameLength();
        supportsCorrelatedSubqueries=src.supportsCorrelatedSubqueries();
        supportsANSI92FullSQL=src.supportsANSI92FullSQL();
        supportsCatalogsInDataManipulation=src.supportsCatalogsInDataManipulation();
        supportsCatalogsInIndexDefinitions=src.supportsCatalogsInIndexDefinitions();
        supportsFullOuterJoins=src.supportsFullOuterJoins();
        supportsGroupByBeyondSelect=src.supportsGroupByBeyondSelect();
        supportsStoredProcedures=src.supportsStoredProcedures();
        supportsOpenCursorsAcrossRollback=src.supportsOpenCursorsAcrossRollback();
        supportsOpenStatementsAcrossCommit=src.supportsOpenStatementsAcrossCommit();
        getMaxColumnsInTable=src.getMaxColumnsInTable();
        supportsLikeEscapeClause=src.supportsLikeEscapeClause();
        supportsMinimumSQLGrammar=src.supportsMinimumSQLGrammar();
        supportsSchemasInDataManipulation=src.supportsSchemasInDataManipulation();
        supportsSubqueriesInIns=src.supportsSubqueriesInIns();
        getMaxCursorNameLength=src.getMaxCursorNameLength();
        getMaxTablesInSelect=src.getMaxTablesInSelect();
        supportsTransactions=src.supportsTransactions();
        supportsLimitedOuterJoins=src.supportsLimitedOuterJoins();
        getMaxCharLiteralLength=src.getMaxCharLiteralLength();
        supportsSubqueriesInExists=src.supportsSubqueriesInExists();
        supportsMixedCaseQuotedIdentifiers=src.supportsMixedCaseQuotedIdentifiers();
        getDatabaseProductName=src.getDatabaseProductName();
        getDriverMinorVersion=src.getDriverMinorVersion();
        supportsAlterTableWithDropColumn=src.supportsAlterTableWithDropColumn();
        nullsAreSortedAtStart=src.nullsAreSortedAtStart();
        nullPlusNonNullIsNull=src.nullPlusNonNullIsNull();
        getSearchStringEscape=src.getSearchStringEscape();
        supportsExpressionsInOrderBy=src.supportsExpressionsInOrderBy();
        getDatabaseProductVersion=src.getDatabaseProductVersion();
        getNumericFunctions=src.getNumericFunctions();
        storesUpperCaseIdentifiers=src.storesUpperCaseIdentifiers();
        usesLocalFilePerTable=src.usesLocalFilePerTable();
        nullsAreSortedAtEnd=src.nullsAreSortedAtEnd();
        supportsMixedCaseIdentifiers=src.supportsMixedCaseIdentifiers();
        storesLowerCaseIdentifiers=src.storesLowerCaseIdentifiers();
        storesMixedCaseQuotedIdentifiers=src.storesMixedCaseQuotedIdentifiers();
        allProceduresAreCallable=src.allProceduresAreCallable();
        storesMixedCaseIdentifiers=src.storesMixedCaseIdentifiers();
        allTablesAreSelectable=src.allTablesAreSelectable();
        storesLowerCaseQuotedIdentifiers=src.storesLowerCaseQuotedIdentifiers();
        getIdentifierQuoteString=src.getIdentifierQuoteString();
        getTimeDateFunctions=src.getTimeDateFunctions();
        supportsAlterTableWithAddColumn=src.supportsAlterTableWithAddColumn();
        getDriverMajorVersion=src.getDriverMajorVersion();
        getExtraNameCharacters=src.getExtraNameCharacters();
        supportsColumnAliasing=src.supportsColumnAliasing();
        supportsTableCorrelationNames=src.supportsTableCorrelationNames();
        storesUpperCaseQuotedIdentifiers=src.storesUpperCaseQuotedIdentifiers();
        generatedKeyAlwaysReturned=src.generatedKeyAlwaysReturned();
        getJDBCMajorVersion=src.getJDBCMajorVersion();
        getResultSetHoldability=src.getResultSetHoldability();
        getMaxLogicalLobSize=src.getMaxLogicalLobSize();
        supportsMultipleOpenResults=src.supportsMultipleOpenResults();
        getDatabaseMajorVersion=src.getDatabaseMajorVersion();
        getDatabaseMinorVersion=src.getDatabaseMinorVersion();
        supportsStatementPooling=src.supportsStatementPooling();
        supportsBatchUpdates=src.supportsBatchUpdates();
        supportsGetGeneratedKeys=src.supportsGetGeneratedKeys();
        supportsNamedParameters=src.supportsNamedParameters();
        getJDBCMinorVersion=src.getJDBCMinorVersion();
        getUserName=src.getUserName();
        nullsAreSortedLow=src.nullsAreSortedLow();
        getDriverName=src.getDriverName();
        supportsConvert=src.supportsConvert();
        getMaxStatements=src.getMaxStatements();
        supportsUnion=src.supportsUnion();
        supportsOuterJoins=src.supportsOuterJoins();
        getSQLKeywords=src.getSQLKeywords();
        supportsUnionAll=src.supportsUnionAll();
        getDriverVersion=src.getDriverVersion();
        getMaxConnections=src.getMaxConnections();
        isCatalogAtStart=src.isCatalogAtStart();
        nullsAreSortedHigh=src.nullsAreSortedHigh();
        getCatalogTerm=src.getCatalogTerm();
        getSQLStateType=src.getSQLStateType();
        locatorsUpdateCopy=src.locatorsUpdateCopy();
        usesLocalFiles=src.usesLocalFiles();
        supportsGroupBy=src.supportsGroupBy();
        getMaxIndexLength=src.getMaxIndexLength();
        getProcedureTerm=src.getProcedureTerm();
        getMaxRowSize=src.getMaxRowSize();
        getSchemaTerm=src.getSchemaTerm();
        supportsSavepoints=src.supportsSavepoints();
        getSystemFunctions=src.getSystemFunctions();
        getStringFunctions=src.getStringFunctions();
        supportsSharding=src.supportsSharding();
        supportsRefCursors=src.supportsRefCursors();
    }

    @Override
    public void serialize(TypedSerializer builder) {
        builder.write("isReadOnly",isReadOnly);
        builder.write("getURL",getURL);
        builder.write("autoCommitFailureClosesAllResultSets",autoCommitFailureClosesAllResultSets);
        builder.write("supportsStoredFunctionsUsingCallSyntax",supportsStoredFunctionsUsingCallSyntax);
        builder.write("supportsSchemasInPrivilegeDefinitions",supportsSchemasInPrivilegeDefinitions);
        builder.write("supportsIntegrityEnhancementFacility",supportsIntegrityEnhancementFacility);
        builder.write("supportsDifferentTableCorrelationNames",supportsDifferentTableCorrelationNames);
        builder.write("supportsOpenStatementsAcrossRollback",supportsOpenStatementsAcrossRollback);
        builder.write("supportsDataManipulationTransactionsOnly",supportsDataManipulationTransactionsOnly);
        builder.write("supportsCatalogsInPrivilegeDefinitions",supportsCatalogsInPrivilegeDefinitions);
        builder.write("dataDefinitionCausesTransactionCommit",dataDefinitionCausesTransactionCommit);
        builder.write("dataDefinitionIgnoredInTransactions",dataDefinitionIgnoredInTransactions);
        builder.write("supportsDataDefinitionAndDataManipulationTransactions",supportsDataDefinitionAndDataManipulationTransactions);
        builder.write("supportsANSI92EntryLevelSQL",supportsANSI92EntryLevelSQL);
        builder.write("supportsSubqueriesInComparisons",supportsSubqueriesInComparisons);
        builder.write("supportsMultipleResultSets",supportsMultipleResultSets);
        builder.write("supportsExtendedSQLGrammar",supportsExtendedSQLGrammar);
        builder.write("supportsSchemasInTableDefinitions",supportsSchemasInTableDefinitions);
        builder.write("supportsOrderByUnrelated",supportsOrderByUnrelated);
        builder.write("supportsNonNullableColumns",supportsNonNullableColumns);
        builder.write("supportsCoreSQLGrammar",supportsCoreSQLGrammar);
        builder.write("supportsCatalogsInProcedureCalls",supportsCatalogsInProcedureCalls);
        builder.write("supportsSelectForUpdate",supportsSelectForUpdate);
        builder.write("supportsMultipleTransactions",supportsMultipleTransactions);
        builder.write("supportsOpenCursorsAcrossCommit",supportsOpenCursorsAcrossCommit);
        builder.write("getMaxColumnNameLength",getMaxColumnNameLength);
        builder.write("getMaxColumnsInIndex",getMaxColumnsInIndex);
        builder.write("getMaxColumnsInSelect",getMaxColumnsInSelect);
        builder.write("getMaxSchemaNameLength",getMaxSchemaNameLength);
        builder.write("getMaxBinaryLiteralLength",getMaxBinaryLiteralLength);
        builder.write("getMaxProcedureNameLength",getMaxProcedureNameLength);
        builder.write("getMaxCatalogNameLength",getMaxCatalogNameLength);
        builder.write("supportsSchemasInIndexDefinitions",supportsSchemasInIndexDefinitions);
        builder.write("getCatalogSeparator",getCatalogSeparator);
        builder.write("supportsSubqueriesInQuantifieds",supportsSubqueriesInQuantifieds);
        builder.write("supportsPositionedDelete",supportsPositionedDelete);
        builder.write("supportsGroupByUnrelated",supportsGroupByUnrelated);
        builder.write("doesMaxRowSizeIncludeBlobs",doesMaxRowSizeIncludeBlobs);
        builder.write("getMaxStatementLength",getMaxStatementLength);
        builder.write("getMaxColumnsInGroupBy",getMaxColumnsInGroupBy);
        builder.write("getMaxUserNameLength",getMaxUserNameLength);
        builder.write("supportsSchemasInProcedureCalls",supportsSchemasInProcedureCalls);
        builder.write("supportsCatalogsInTableDefinitions",supportsCatalogsInTableDefinitions);
        builder.write("supportsPositionedUpdate",supportsPositionedUpdate);
        builder.write("getDefaultTransactionIsolation",getDefaultTransactionIsolation);
        builder.write("getMaxColumnsInOrderBy",getMaxColumnsInOrderBy);
        builder.write("supportsANSI92IntermediateSQL",supportsANSI92IntermediateSQL);
        builder.write("getMaxTableNameLength",getMaxTableNameLength);
        builder.write("supportsCorrelatedSubqueries",supportsCorrelatedSubqueries);
        builder.write("supportsANSI92FullSQL",supportsANSI92FullSQL);
        builder.write("supportsCatalogsInDataManipulation",supportsCatalogsInDataManipulation);
        builder.write("supportsCatalogsInIndexDefinitions",supportsCatalogsInIndexDefinitions);
        builder.write("supportsFullOuterJoins",supportsFullOuterJoins);
        builder.write("supportsGroupByBeyondSelect",supportsGroupByBeyondSelect);
        builder.write("supportsStoredProcedures",supportsStoredProcedures);
        builder.write("supportsOpenCursorsAcrossRollback",supportsOpenCursorsAcrossRollback);
        builder.write("supportsOpenStatementsAcrossCommit",supportsOpenStatementsAcrossCommit);
        builder.write("getMaxColumnsInTable",getMaxColumnsInTable);
        builder.write("supportsLikeEscapeClause",supportsLikeEscapeClause);
        builder.write("supportsMinimumSQLGrammar",supportsMinimumSQLGrammar);
        builder.write("supportsSchemasInDataManipulation",supportsSchemasInDataManipulation);
        builder.write("supportsSubqueriesInIns",supportsSubqueriesInIns);
        builder.write("getMaxCursorNameLength",getMaxCursorNameLength);
        builder.write("getMaxTablesInSelect",getMaxTablesInSelect);
        builder.write("supportsTransactions",supportsTransactions);
        builder.write("supportsLimitedOuterJoins",supportsLimitedOuterJoins);
        builder.write("getMaxCharLiteralLength",getMaxCharLiteralLength);
        builder.write("supportsSubqueriesInExists",supportsSubqueriesInExists);
        builder.write("supportsMixedCaseQuotedIdentifiers",supportsMixedCaseQuotedIdentifiers);
        builder.write("getDatabaseProductName",getDatabaseProductName);
        builder.write("getDriverMinorVersion",getDriverMinorVersion);
        builder.write("supportsAlterTableWithDropColumn",supportsAlterTableWithDropColumn);
        builder.write("nullsAreSortedAtStart",nullsAreSortedAtStart);
        builder.write("nullPlusNonNullIsNull",nullPlusNonNullIsNull);
        builder.write("getSearchStringEscape",getSearchStringEscape);
        builder.write("supportsExpressionsInOrderBy",supportsExpressionsInOrderBy);
        builder.write("getDatabaseProductVersion",getDatabaseProductVersion);
        builder.write("getNumericFunctions",getNumericFunctions);
        builder.write("storesUpperCaseIdentifiers",storesUpperCaseIdentifiers);
        builder.write("usesLocalFilePerTable",usesLocalFilePerTable);
        builder.write("nullsAreSortedAtEnd",nullsAreSortedAtEnd);
        builder.write("supportsMixedCaseIdentifiers",supportsMixedCaseIdentifiers);
        builder.write("storesLowerCaseIdentifiers",storesLowerCaseIdentifiers);
        builder.write("storesMixedCaseQuotedIdentifiers",storesMixedCaseQuotedIdentifiers);
        builder.write("allProceduresAreCallable",allProceduresAreCallable);
        builder.write("storesMixedCaseIdentifiers",storesMixedCaseIdentifiers);
        builder.write("allTablesAreSelectable",allTablesAreSelectable);
        builder.write("storesLowerCaseQuotedIdentifiers",storesLowerCaseQuotedIdentifiers);
        builder.write("getIdentifierQuoteString",getIdentifierQuoteString);
        builder.write("getTimeDateFunctions",getTimeDateFunctions);
        builder.write("supportsAlterTableWithAddColumn",supportsAlterTableWithAddColumn);
        builder.write("getDriverMajorVersion",getDriverMajorVersion);
        builder.write("getExtraNameCharacters",getExtraNameCharacters);
        builder.write("supportsColumnAliasing",supportsColumnAliasing);
        builder.write("supportsTableCorrelationNames",supportsTableCorrelationNames);
        builder.write("storesUpperCaseQuotedIdentifiers",storesUpperCaseQuotedIdentifiers);
        builder.write("generatedKeyAlwaysReturned",generatedKeyAlwaysReturned);
        builder.write("getJDBCMajorVersion",getJDBCMajorVersion);
        builder.write("getResultSetHoldability",getResultSetHoldability);
        builder.write("getMaxLogicalLobSize",getMaxLogicalLobSize);
        builder.write("supportsMultipleOpenResults",supportsMultipleOpenResults);
        builder.write("getDatabaseMajorVersion",getDatabaseMajorVersion);
        builder.write("getDatabaseMinorVersion",getDatabaseMinorVersion);
        builder.write("supportsStatementPooling",supportsStatementPooling);
        builder.write("supportsBatchUpdates",supportsBatchUpdates);
        builder.write("supportsGetGeneratedKeys",supportsGetGeneratedKeys);
        builder.write("supportsNamedParameters",supportsNamedParameters);
        builder.write("getJDBCMinorVersion",getJDBCMinorVersion);
        builder.write("getUserName",getUserName);
        builder.write("nullsAreSortedLow",nullsAreSortedLow);
        builder.write("getDriverName",getDriverName);
        builder.write("supportsConvert",supportsConvert);
        builder.write("getMaxStatements",getMaxStatements);
        builder.write("supportsUnion",supportsUnion);
        builder.write("supportsOuterJoins",supportsOuterJoins);
        builder.write("getSQLKeywords",getSQLKeywords);
        builder.write("supportsUnionAll",supportsUnionAll);
        builder.write("getDriverVersion",getDriverVersion);
        builder.write("getMaxConnections",getMaxConnections);
        builder.write("isCatalogAtStart",isCatalogAtStart);
        builder.write("nullsAreSortedHigh",nullsAreSortedHigh);
        builder.write("getCatalogTerm",getCatalogTerm);
        builder.write("getSQLStateType",getSQLStateType);
        builder.write("locatorsUpdateCopy",locatorsUpdateCopy);
        builder.write("usesLocalFiles",usesLocalFiles);
        builder.write("supportsGroupBy",supportsGroupBy);
        builder.write("getMaxIndexLength",getMaxIndexLength);
        builder.write("getProcedureTerm",getProcedureTerm);
        builder.write("getMaxRowSize",getMaxRowSize);
        builder.write("getSchemaTerm",getSchemaTerm);
        builder.write("supportsSavepoints",supportsSavepoints);
        builder.write("getSystemFunctions",getSystemFunctions);
        builder.write("getStringFunctions",getStringFunctions);
        builder.write("supportsSharding",supportsSharding);
        builder.write("supportsRefCursors",supportsRefCursors);
        builder.write("traceId",traceId);
    }

    @Override
    public JdbcResult deserialize(TypedSerializer builder) {

        isReadOnly =builder.read("isReadOnly");
        getURL =builder.read("getURL");
        autoCommitFailureClosesAllResultSets =builder.read("autoCommitFailureClosesAllResultSets");
        supportsStoredFunctionsUsingCallSyntax =builder.read("supportsStoredFunctionsUsingCallSyntax");
        supportsSchemasInPrivilegeDefinitions =builder.read("supportsSchemasInPrivilegeDefinitions");
        supportsIntegrityEnhancementFacility =builder.read("supportsIntegrityEnhancementFacility");
        supportsDifferentTableCorrelationNames =builder.read("supportsDifferentTableCorrelationNames");
        supportsOpenStatementsAcrossRollback =builder.read("supportsOpenStatementsAcrossRollback");
        supportsDataManipulationTransactionsOnly =builder.read("supportsDataManipulationTransactionsOnly");
        supportsCatalogsInPrivilegeDefinitions =builder.read("supportsCatalogsInPrivilegeDefinitions");
        dataDefinitionCausesTransactionCommit =builder.read("dataDefinitionCausesTransactionCommit");
        dataDefinitionIgnoredInTransactions =builder.read("dataDefinitionIgnoredInTransactions");
        supportsDataDefinitionAndDataManipulationTransactions =builder.read("supportsDataDefinitionAndDataManipulationTransactions");
        supportsANSI92EntryLevelSQL =builder.read("supportsANSI92EntryLevelSQL");
        supportsSubqueriesInComparisons =builder.read("supportsSubqueriesInComparisons");
        supportsMultipleResultSets =builder.read("supportsMultipleResultSets");
        supportsExtendedSQLGrammar =builder.read("supportsExtendedSQLGrammar");
        supportsSchemasInTableDefinitions =builder.read("supportsSchemasInTableDefinitions");
        supportsOrderByUnrelated =builder.read("supportsOrderByUnrelated");
        supportsNonNullableColumns =builder.read("supportsNonNullableColumns");
        supportsCoreSQLGrammar =builder.read("supportsCoreSQLGrammar");
        supportsCatalogsInProcedureCalls =builder.read("supportsCatalogsInProcedureCalls");
        supportsSelectForUpdate =builder.read("supportsSelectForUpdate");
        supportsMultipleTransactions =builder.read("supportsMultipleTransactions");
        supportsOpenCursorsAcrossCommit =builder.read("supportsOpenCursorsAcrossCommit");
        getMaxColumnNameLength =builder.read("getMaxColumnNameLength");
        getMaxColumnsInIndex =builder.read("getMaxColumnsInIndex");
        getMaxColumnsInSelect =builder.read("getMaxColumnsInSelect");
        getMaxSchemaNameLength =builder.read("getMaxSchemaNameLength");
        getMaxBinaryLiteralLength =builder.read("getMaxBinaryLiteralLength");
        getMaxProcedureNameLength =builder.read("getMaxProcedureNameLength");
        getMaxCatalogNameLength =builder.read("getMaxCatalogNameLength");
        supportsSchemasInIndexDefinitions =builder.read("supportsSchemasInIndexDefinitions");
        getCatalogSeparator =builder.read("getCatalogSeparator");
        supportsSubqueriesInQuantifieds =builder.read("supportsSubqueriesInQuantifieds");
        supportsPositionedDelete =builder.read("supportsPositionedDelete");
        supportsGroupByUnrelated =builder.read("supportsGroupByUnrelated");
        doesMaxRowSizeIncludeBlobs =builder.read("doesMaxRowSizeIncludeBlobs");
        getMaxStatementLength =builder.read("getMaxStatementLength");
        getMaxColumnsInGroupBy =builder.read("getMaxColumnsInGroupBy");
        getMaxUserNameLength =builder.read("getMaxUserNameLength");
        supportsSchemasInProcedureCalls =builder.read("supportsSchemasInProcedureCalls");
        supportsCatalogsInTableDefinitions =builder.read("supportsCatalogsInTableDefinitions");
        supportsPositionedUpdate =builder.read("supportsPositionedUpdate");
        getDefaultTransactionIsolation =builder.read("getDefaultTransactionIsolation");
        getMaxColumnsInOrderBy =builder.read("getMaxColumnsInOrderBy");
        supportsANSI92IntermediateSQL =builder.read("supportsANSI92IntermediateSQL");
        getMaxTableNameLength =builder.read("getMaxTableNameLength");
        supportsCorrelatedSubqueries =builder.read("supportsCorrelatedSubqueries");
        supportsANSI92FullSQL =builder.read("supportsANSI92FullSQL");
        supportsCatalogsInDataManipulation =builder.read("supportsCatalogsInDataManipulation");
        supportsCatalogsInIndexDefinitions =builder.read("supportsCatalogsInIndexDefinitions");
        supportsFullOuterJoins =builder.read("supportsFullOuterJoins");
        supportsGroupByBeyondSelect =builder.read("supportsGroupByBeyondSelect");
        supportsStoredProcedures =builder.read("supportsStoredProcedures");
        supportsOpenCursorsAcrossRollback =builder.read("supportsOpenCursorsAcrossRollback");
        supportsOpenStatementsAcrossCommit =builder.read("supportsOpenStatementsAcrossCommit");
        getMaxColumnsInTable =builder.read("getMaxColumnsInTable");
        supportsLikeEscapeClause =builder.read("supportsLikeEscapeClause");
        supportsMinimumSQLGrammar =builder.read("supportsMinimumSQLGrammar");
        supportsSchemasInDataManipulation =builder.read("supportsSchemasInDataManipulation");
        supportsSubqueriesInIns =builder.read("supportsSubqueriesInIns");
        getMaxCursorNameLength =builder.read("getMaxCursorNameLength");
        getMaxTablesInSelect =builder.read("getMaxTablesInSelect");
        supportsTransactions =builder.read("supportsTransactions");
        supportsLimitedOuterJoins =builder.read("supportsLimitedOuterJoins");
        getMaxCharLiteralLength =builder.read("getMaxCharLiteralLength");
        supportsSubqueriesInExists =builder.read("supportsSubqueriesInExists");
        supportsMixedCaseQuotedIdentifiers =builder.read("supportsMixedCaseQuotedIdentifiers");
        getDatabaseProductName =builder.read("getDatabaseProductName");
        getDriverMinorVersion =builder.read("getDriverMinorVersion");
        supportsAlterTableWithDropColumn =builder.read("supportsAlterTableWithDropColumn");
        nullsAreSortedAtStart =builder.read("nullsAreSortedAtStart");
        nullPlusNonNullIsNull =builder.read("nullPlusNonNullIsNull");
        getSearchStringEscape =builder.read("getSearchStringEscape");
        supportsExpressionsInOrderBy =builder.read("supportsExpressionsInOrderBy");
        getDatabaseProductVersion =builder.read("getDatabaseProductVersion");
        getNumericFunctions =builder.read("getNumericFunctions");
        storesUpperCaseIdentifiers =builder.read("storesUpperCaseIdentifiers");
        usesLocalFilePerTable =builder.read("usesLocalFilePerTable");
        nullsAreSortedAtEnd =builder.read("nullsAreSortedAtEnd");
        supportsMixedCaseIdentifiers =builder.read("supportsMixedCaseIdentifiers");
        storesLowerCaseIdentifiers =builder.read("storesLowerCaseIdentifiers");
        storesMixedCaseQuotedIdentifiers =builder.read("storesMixedCaseQuotedIdentifiers");
        allProceduresAreCallable =builder.read("allProceduresAreCallable");
        storesMixedCaseIdentifiers =builder.read("storesMixedCaseIdentifiers");
        allTablesAreSelectable =builder.read("allTablesAreSelectable");
        storesLowerCaseQuotedIdentifiers =builder.read("storesLowerCaseQuotedIdentifiers");
        getIdentifierQuoteString =builder.read("getIdentifierQuoteString");
        getTimeDateFunctions =builder.read("getTimeDateFunctions");
        supportsAlterTableWithAddColumn =builder.read("supportsAlterTableWithAddColumn");
        getDriverMajorVersion =builder.read("getDriverMajorVersion");
        getExtraNameCharacters =builder.read("getExtraNameCharacters");
        supportsColumnAliasing =builder.read("supportsColumnAliasing");
        supportsTableCorrelationNames =builder.read("supportsTableCorrelationNames");
        storesUpperCaseQuotedIdentifiers =builder.read("storesUpperCaseQuotedIdentifiers");
        generatedKeyAlwaysReturned =builder.read("generatedKeyAlwaysReturned");
        getJDBCMajorVersion =builder.read("getJDBCMajorVersion");
        getResultSetHoldability =builder.read("getResultSetHoldability");
        getMaxLogicalLobSize =builder.read("getMaxLogicalLobSize");
        supportsMultipleOpenResults =builder.read("supportsMultipleOpenResults");
        getDatabaseMajorVersion =builder.read("getDatabaseMajorVersion");
        getDatabaseMinorVersion =builder.read("getDatabaseMinorVersion");
        supportsStatementPooling =builder.read("supportsStatementPooling");
        supportsBatchUpdates =builder.read("supportsBatchUpdates");
        supportsGetGeneratedKeys =builder.read("supportsGetGeneratedKeys");
        supportsNamedParameters =builder.read("supportsNamedParameters");
        getJDBCMinorVersion =builder.read("getJDBCMinorVersion");
        getUserName =builder.read("getUserName");
        nullsAreSortedLow =builder.read("nullsAreSortedLow");
        getDriverName =builder.read("getDriverName");
        supportsConvert =builder.read("supportsConvert");
        getMaxStatements =builder.read("getMaxStatements");
        supportsUnion =builder.read("supportsUnion");
        supportsOuterJoins =builder.read("supportsOuterJoins");
        getSQLKeywords =builder.read("getSQLKeywords");
        supportsUnionAll =builder.read("supportsUnionAll");
        getDriverVersion =builder.read("getDriverVersion");
        getMaxConnections =builder.read("getMaxConnections");
        isCatalogAtStart =builder.read("isCatalogAtStart");
        nullsAreSortedHigh =builder.read("nullsAreSortedHigh");
        getCatalogTerm =builder.read("getCatalogTerm");
        getSQLStateType =builder.read("getSQLStateType");
        locatorsUpdateCopy =builder.read("locatorsUpdateCopy");
        usesLocalFiles =builder.read("usesLocalFiles");
        supportsGroupBy =builder.read("supportsGroupBy");
        getMaxIndexLength =builder.read("getMaxIndexLength");
        getProcedureTerm =builder.read("getProcedureTerm");
        getMaxRowSize =builder.read("getMaxRowSize");
        getSchemaTerm =builder.read("getSchemaTerm");
        supportsSavepoints =builder.read("supportsSavepoints");
        getSystemFunctions =builder.read("getSystemFunctions");
        getStringFunctions =builder.read("getStringFunctions");
        supportsSharding =builder.read("supportsSharding");
        supportsRefCursors =builder.read("supportsRefCursors");
        traceId =builder.read("traceId");
        return this;
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

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsConvert")
                        .withTypes(int.class,int.class)
                        .withParameters(fromType,toType)
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
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return ((ObjectResult)engine.execute(new Exec(this,
                        "supportsResultSetHoldability")
                        .withTypes(int.class)
                        .withParameters(holdability)
                ,connection.getTraceId(),getTraceId())).getResult();
    }

    public void initialize(JdbcConnection jdbcConnection, Engine engine) {
        this.connection=jdbcConnection;
        this.engine=engine;
    }
}