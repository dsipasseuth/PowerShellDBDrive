using System;
using System.Management.Automation;
using System.Data.Common;
using System.Collections.Generic;
using PowerShellDBDrive.DataModel;
using GodSharp.Data.Common.DbProvider;

namespace PowerShellDBDrive.Drives
{
    /// <summary> 
    /// Interface used by provider to access database functionality.
    /// </summary>
    public interface IDatabaseDriveInfo
    {

        /// <summary> 
        /// Return all schemas information. 
        /// </summary> 
        /// <returns>Collection of schema information objects.</returns> 
        IEnumerable<IDatabaseSchemaInfo> GetSchemas();

        /// <summary> 
        /// Return all schemas information. 
        /// </summary> 
        /// <returns>Collection of schema information objects.</returns> 
        IEnumerable<String> GetSchemasNames();

        /// <summary> 
        /// Return given schema information.
        /// </summary>
        /// <param name="schemaName">the schema name</param>
        /// <returns>Schema information objects.</returns>
        IDatabaseSchemaInfo GetSchema(string schemaName);

        /// <summary> 
        /// Retrieve the list of rows from given schema table database. 
        /// </summary> 
        /// <param name="schemaName">the schema name</param>
        /// <returns>
        /// Collection of DatabaseTableInfo objects, each object representing information about one table
        /// </returns> 
        IEnumerable<IDatabaseTableInfo> GetTables(string schemaName);

        /// <summary> 
        /// Retrieve the table information from given schema table database.
        /// </summary> 
        /// <param name="schemaName">the schema name</param>
        /// <param name="tableName">the table to query</param>
        /// <returns>
        /// A DatabaseTableInfo object representing information about one table
        /// </returns> 
        IDatabaseTableInfo GetTable(string schemaName, string tableName);

        /// <summary> 
        /// Retrieve the list of rows from given schema table database. 
        /// </summary> 
        /// <param name="schemaName">the schema name</param>
        /// <param name="tableName">the table to query</param>
        /// <param name="maxResult">max row to returns</param>
        /// <returns>
        /// Collection of PSObject objects, each object representing information about one row
        /// </returns> 
        IEnumerable<PSObject> GetRows(string schemaName, string tableName, int maxResult);
    }

    public static class DatabaseDriveInfoFactory
    {
        public static DatabaseDriveInfo NewInstance(PSDriveInfo driveInfo, DatabaseParameters parameters)
        {
            switch (parameters.Provider)
            {
                case "Oracle.ManagedDataAccess.Client":
                    return new OracleDatabaseDriveInfo(driveInfo, parameters);
                case "NPGSQL":
                    return new PgDatabaseDriveInfo(driveInfo, parameters);
                default: throw new ArgumentException(String.Format("{0} provider is not supported yet !", parameters.Provider));
            }
        }
    }

    /// <summary> 
    /// Base class that can be used to implement IDatabaseDriveInfo by provider to access database functionality.
    /// </summary>
    public abstract class DatabaseDriveInfo : PSDriveInfo, IDatabaseDriveInfo
    {
        public const int DEFAULT_MAX_READ_RESULT = 100;

        public const int DEFAULT_BULK_READ_LIMIT = 50;

        public const int DEFAULT_TIMEOUT = 60;

        public int MaxReadResult { get; set; }

        public int BulkReadLimit { get; set; }

        public int Timeout { get; set; }

        public string ParsedConnectionString { get; protected set; }

        private DatabaseParameters Parameters { get; set; }

        private DbProviderFactory Factory { get; set; }

        public DatabaseDriveInfo(PSDriveInfo driveInfo, DatabaseParameters parameters) : base(driveInfo)
        {
            MaxReadResult = DEFAULT_MAX_READ_RESULT;
            BulkReadLimit = DEFAULT_BULK_READ_LIMIT;
            Timeout = DEFAULT_TIMEOUT;
            Factory = DbProviderFactories.GetFactory(parameters.Provider);
            DbConnectionStringBuilder csb = Factory.CreateConnectionStringBuilder();
            csb.ConnectionString = parameters.ConnectionString;
            ParsedConnectionString = csb.ConnectionString;
        }

        /// <summary>
        /// Return a new connection.
        /// </summary>
        /// <returns></returns>
        public DbConnection GetConnection()
        {
            DbConnection connection = Factory.CreateConnection();
            connection.ConnectionString = ParsedConnectionString;
            return connection;
        }

        /// <summary>
        /// The root drive name. 
        /// The root is the drive name with colon, and the path separator (like FileSystem provider)
        /// If Root is empty or null, it will return "drivename:\"
        /// If Root is not empty, then it will return "drivename:\Root\Path"
        /// </summary>
        /// <returns>The root drive name.</returns>
        public string GetRootDrive()
        {
            if (string.IsNullOrEmpty(Root))
            {
                return Name + ":" + DatabaseUtils.PATH_SEPARATOR;
            }
            return Name + ":" + DatabaseUtils.PATH_SEPARATOR + Root + DatabaseUtils.PATH_SEPARATOR;
        }

        /// <summary> 
        /// Retrieve the list of tables from the database. 
        /// </summary> 
        /// <returns> 
        /// <param name="schemaName">schema name</param>
        /// <param name="tableName">the table name</param>
        /// Collection of DatabaseTableInfo objects, each object representing 
        /// information about one database table
        /// </returns>
        public IEnumerable<PSObject> GetRows(string schemaName, string tableName)
        {
            foreach (PSObject p in GetRows(schemaName, tableName, MaxReadResult))
            {
                yield return p;
            }
        }

        /// TODO Rewrite to make select statement.
        /// <summary> 
        /// Retrieves a single row from the named table. 
        /// </summary> 
        /// <param name="tableName">The table that contains the  
        /// numbered row.</param> 
        /// <param name="row">The index of the row to return.</param> 
        /// <returns>The specified table row.</returns> 
        public PSObject GetRow(string schemaName, string tableName, string row)
        {
            // WriteError(new ErrorRecord(new ItemNotFoundException(), "RowNotFound", ErrorCategory.ObjectNotFound, row)); 
            return null;
        }

        #region IDatabaseDriveInfo Methods

        public abstract IEnumerable<IDatabaseSchemaInfo> GetSchemas();

        public abstract IEnumerable<String> GetSchemasNames();
		
		public abstract IEnumerable<String> GetSchemasNames(string regexp);

        public abstract IDatabaseSchemaInfo GetSchema(string schemaName);

        public abstract IEnumerable<ObjectType> GetSupportedObjectTypes(string schemaName);

        public abstract IEnumerable<IDatabaseViewInfo> GetViews(string schemaName);

        public abstract IEnumerable<String> GetViewsNames(string schemaName);

        public abstract IEnumerable<String> GetViewsNames(string schemaName, string viewName);

        public abstract IDatabaseViewInfo GetView(string schemaName, string viewName);

        public abstract IEnumerable<IDatabaseTableInfo> GetTables(string schemaName);

        public abstract IEnumerable<String> GetTablesNames(string schemaName);
		
		public abstract IEnumerable<String> GetTablesNames(string schemaName, string tableName);
		
        public abstract IDatabaseTableInfo GetTable(string schemaName, string tableName);

        public abstract IEnumerable<PSObject> GetRows(string schemaName, string tableName, int maxResult);

        public abstract bool IsSchemaExist(string schemaName);

        public abstract bool IsObjectExist(string schemaName, ObjectType objectType, string[] objectPath);

        #endregion IDatabaseDriveInfo Methods
    }
}