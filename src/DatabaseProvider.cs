using System;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using PowerShellDBDrive.Drives;
using PowerShellDBDrive.DataModel;

namespace PowerShellDBDrive
{
    [CmdletProvider("DatabaseProvider", ProviderCapabilities.None)]
    public class DatabaseProvider : NavigationCmdletProvider
    {
        #region Drive Manipulation 

        /// <summary> 
        /// The Windows PowerShell engine calls this method when the New-Drive  
        /// cmdlet is run. This provider creates a connection to the database  
        /// file and sets the Connection property in the PSDriveInfo. 
        /// </summary> 
        /// <param name="drive"> 
        /// Information describing the drive to create. 
        /// </param> 
        /// <returns>An object that describes the new drive.</returns> 
        protected override PSDriveInfo NewDrive(PSDriveInfo drive)
        {
            // Check to see if the supplied drive object is null. 
            if (drive == null)
            {
                WriteError(new ErrorRecord(
                                       new ArgumentNullException("drive"),
                                       "NullDrive",
                                       ErrorCategory.InvalidArgument,
                                       null));
                return null;
            }

            if (drive.Root.Equals(DatabaseUtils.PATH_SEPARATOR))
            {
                WriteError(new ErrorRecord(new ArgumentException("Root cannot be path separator"), "BadRoot", ErrorCategory.InvalidArgument, null));
                return null;
            }
            var driveParams = this.DynamicParameters as DatabaseParameters;
            DatabaseDriveInfo driveInfo = DatabaseDriveInfoFactory.NewInstance(drive, driveParams);
            WriteDebug(String.Format("Parsed Connection String : {0}", driveInfo.ParsedConnectionString));
            return driveInfo;
        }

        /// <summary> 
        /// The Windows PowerShell engine calls this method when the  
        /// Remove-Drive cmdlet is run. 
        /// </summary> 
        /// <param name="drive">The drive to remove.</param> 
        /// <returns>The drive to be removed.</returns> 
        protected override PSDriveInfo RemoveDrive(PSDriveInfo drive)
        {
            // Check to see if the supplied drive object is null. 
            if (drive == null)
            {
                WriteError(new ErrorRecord(new ArgumentNullException("drive"), "NullDrive", ErrorCategory.InvalidArgument, drive));
                return null;
            }
            DatabaseDriveInfo driveInfo = drive as DatabaseDriveInfo;
            if (driveInfo == null)
            {
                return null;
            }
            return driveInfo;
        }

        protected override object NewDriveDynamicParameters()
        {
            return new DatabaseParameters();
        }

        #endregion Drive Manipulation

        #region Item Methods

        /// <summary> 
        /// The Windows PowerShell engine calls this method when the Get-Item  
        /// cmdlet is run. 
        /// </summary> 
        /// <param name="path">The path to the item to return.</param> 
        protected override void GetItem(string path)
        {
            WriteVerbose(string.Format("GetItem: <- Path='{0}'", path));
            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return;
            }

            PathDescriptor pathDescriptor = new PathDescriptor(path);

            switch (pathDescriptor.PathType)
            {
                case PathType.Database:
                    WriteVerbose("GetItem: -> Database");
                    WriteItemObject(PSDriveInfo, di.GetRootDrive(), true);
                    break;
                case PathType.Schema:
                    IDatabaseSchemaInfo schema = di.GetSchema(pathDescriptor.SchemaName);
                    WriteVerbose("GetItem: -> Schema");
                    WriteItemObject(schema, path, true);
                    break;
                case PathType.ObjectType:
                    WriteVerbose("GetItem: -> ObjectType");
                    WriteItemObject(pathDescriptor.DatabaseObjectType, path, true);
                    break;
                case PathType.Object:
                    switch (pathDescriptor.DatabaseObjectType)
                    {
                        case ObjectType.TABLE:
                            {
                                IDatabaseTableInfo table = di.GetTable(pathDescriptor.SchemaName, pathDescriptor.ObjectPath[0]);
                                WriteVerbose("GetItem: -> Object - Table");
                                WriteItemObject(table, path, true);
                                break;
                            }
                        case ObjectType.VIEW:
                            {
                                IDatabaseViewInfo table = di.GetView(pathDescriptor.SchemaName, pathDescriptor.ObjectPath[0]);
                                WriteVerbose("GetItem: -> Object - View");
                                WriteItemObject(table, path, true);
                                break;
                            }
                        default:
                            ThrowTerminatingInvalidPathException(path);
                            break;
                    }
                    break;
                case PathType.Row:
                    WriteVerbose("GetItem: -> Row");
                    /// WriteItemObject(row, path, false);
                    break;
                default:
                    ThrowTerminatingInvalidPathException(path);
                    break;
            }
        }

        /// <summary> 
        /// Test to see if the specified path is syntactically valid. 
        /// </summary> 
        /// <param name="path">The path to validate.</param> 
        /// <returns>True if the specified path is valid.</returns> 
        protected override bool IsValidPath(string path)
        {
            // Check to see if the path is null or empty. 
            WriteVerbose(string.Format("IsValidPath:{0}", path));
            if (string.IsNullOrEmpty(path))
            {
                return false;
            }
            return DatabaseUtils.PATH_VALIDATOR.IsMatch(path);
        }

        /// <summary> 
        /// Test to see if the specified item exists. 
        /// </summary> 
        /// <param name="path">The path to the item to verify.</param> 
        /// <returns>True if the item is found.</returns> 
        protected override bool ItemExists(string path)
        {
            WriteVerbose(string.Format("ItemExists: <- Path='{0}'", path));
            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                WriteVerbose("ItemExists: -> false");
                return false;
            }

            PathDescriptor pathDescriptor = new PathDescriptor(path);
            
            switch (pathDescriptor.PathType)
            {
                case PathType.Database:
                    {
                        WriteVerbose("ItemExists: {PathType.Database} -> true");
                        return true;
                    }
                case PathType.Schema:
                    {
                        bool result = di.IsSchemaExist(pathDescriptor.SchemaName);
                        WriteVerbose(string.Format("ItemExists: {{PathType.Schema}} -> {0}", result));
                        return result;
                    }
                case PathType.ObjectType:
                    {
                        WriteVerbose("ItemExists: {PathType.ObjectType} -> true");
                        return true;
                    }
                case PathType.Object:
                    {
                        bool result = di.IsObjectExist(pathDescriptor.SchemaName, pathDescriptor.DatabaseObjectType, pathDescriptor.ObjectPath);
                        WriteVerbose(string.Format("ItemExists: {{PathType.Object}} -> {0},{1}", pathDescriptor.ObjectPath, result));
                        return result;
                    }
                case PathType.Row:
                    {
                        WriteVerbose("ItemExists: {PathType.Row} -> false");
                        return false;
                    }
                default:
                    WriteVerbose("ItemExists: {PathType.Invalid} -> false");
                    return false;
            }
        }

        /// <summary> 
        /// Checks to see if the table name is valid. 
        /// </summary> 
        /// <param name="tableName">Table name to validate</param> 
        /// <remarks>Helps to check for SQL injection attacks</remarks> 
        /// <returns>A Boolean value indicating true if the name is valid.</returns> 
        private bool TableNameIsValid(string tableName)
        {
            if (!DatabaseUtils.NameIsValid(tableName))
            {
                WriteError(new ErrorRecord(
                                    new ArgumentException("Table name not valid"),
                                    "TableNameNotValid",
                                    ErrorCategory.InvalidArgument,
                                    tableName));
                return false;
            }
            return true;
        }


        /// <summary> 
        /// Checks to see if the schema name is valid. 
        /// </summary> 
        /// <param name="schemaName">Schema name to validate</param> 
        /// <remarks>Helps to check for SQL injection attacks</remarks> 
        /// <returns>A Boolean value indicating true if the name is valid.</returns> 
        private bool SchemaNameIsValid(string schemaName)
        {
            if (!DatabaseUtils.NameIsValid(schemaName))
            {
                WriteError(new ErrorRecord(
                                     new ArgumentException("Schema name not valid"),
                                     "SchemaNameNotValid",
                                     ErrorCategory.InvalidArgument,
                                     schemaName));
                return false;
            }
            return true;
        }

        #endregion Item Methods

        #region Container Methods

        /// <summary> 
        /// The Windows PowerShell engine calls this method when the Get-ChildItem  
        /// cmdlet is run. This provider returns either the tables in the database  
        /// or the rows of the table. 
        /// </summary> 
        /// <param name="path">The path to the parent item.</param> 
        /// <param name="recurse">A Boolean value that indicates true to return all  
        /// child items recursively. 
        /// </param> 
        protected override void GetChildItems(string path, bool recurse)
        {
            WriteVerbose(string.Format("GetChildItems: <- Path='{0}', Recurse='{1}'", path, recurse));
            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return;
            }
            PathDescriptor pathDescriptor = new PathDescriptor(path);

            switch (pathDescriptor.PathType)
            {
                case PathType.Database:
                    WriteVerbose("GetChildItems: -- Database");
                    foreach (IDatabaseSchemaInfo schema in di.GetSchemas())
                    {
                        WriteVerbose(string.Format("GetChildItems: ---> Database schema '{0}'", schema.SchemaName));
                        string outputPath = di.GetRootDrive() + schema.SchemaName;
                        WriteItemObject(schema, outputPath, true);
                        if (recurse)
                        {
                            GetChildItems(outputPath, recurse);
                        }
                    }
                    WriteVerbose("GetChildItems: -- Database Done");
                    break;
                case PathType.Schema:
                    WriteVerbose("GetChildItems: -- Schema");
                    foreach (ObjectType objectType in di.GetSupportedObjectTypes(pathDescriptor.SchemaName))
                    {
                        string outputPath = di.GetRootDrive() + pathDescriptor.SchemaName + DatabaseUtils.PATH_SEPARATOR + objectType;
                        WriteItemObject(objectType, outputPath, false);
                    }
                    WriteVerbose("GetChildItems: -- Schema Done");
                    break;
                case PathType.ObjectType:
                    WriteVerbose("GetChildItems: -- ObjectType");
                    foreach (IDatabaseTableInfo table in di.GetTables(pathDescriptor.SchemaName))
                    {
                        WriteVerbose(string.Format("GetChildItems: ---> Database table '{0}'", table.TableName));
                        string outputPath = di.GetRootDrive() + pathDescriptor.SchemaName + DatabaseUtils.PATH_SEPARATOR + pathDescriptor.DatabaseObjectType + DatabaseUtils.PATH_SEPARATOR + table.TableName;
                        WriteItemObject(table, outputPath, true);
                        if (recurse)
                        {
                            GetChildItems(outputPath, recurse);
                        }
                    }
                    WriteVerbose("GetChildItems: -- ObjectType Done");
                    break;
                case PathType.Object:
                    WriteVerbose("GetChildItems: -- Table");
                    foreach (PSObject row in di.GetRows(pathDescriptor.SchemaName, pathDescriptor.ObjectPath[0]))
                    {
                        WriteItemObject(row, path, false);
                    }
                    WriteVerbose("GetChildItems: -- Table Done");
                    break;
                default:
                    ThrowTerminatingInvalidPathException(path);
                    break;
            }
        }

        /// <summary> 
        /// Return the names of all child items. 
        /// </summary> 
        /// <param name="path">The root path.</param> 
        /// <param name="returnContainers">This parameter is not used.</param> 
        protected override void GetChildNames(string path, ReturnContainers returnContainers)
        {
            WriteVerbose(string.Format("GetChildNames: <- Path='{0}'", path));
            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return;
            }

            PathDescriptor pathDescriptor = new PathDescriptor(path);

            switch (pathDescriptor.PathType)
            {
                case PathType.Database:
                    {
                        foreach (IDatabaseSchemaInfo schema in di.GetSchemas())
                        {
                            WriteItemObject(schema.SchemaName, path, true);
                        }
                        break;
                    }
                case PathType.Schema:
                    {
                        foreach (IDatabaseTableInfo table in di.GetTables(pathDescriptor.SchemaName))
                        {
                            WriteItemObject(table.TableName, path, true);
                        }
                        break;
                    }
                case PathType.ObjectType:
                    {
                        foreach (ObjectType objectType in di.GetSupportedObjectTypes(pathDescriptor.SchemaName))
                        {
                            WriteItemObject(objectType.GetTypeCode(), path, true);
                        }
                        break;
                    }
                case PathType.Object:
                    foreach (PSObject row in di.GetRows(pathDescriptor.SchemaName, pathDescriptor.ObjectPath[0]))
                    {
                        /// TODO WriteItemObject(row.Properties[], path, false); 
                    }
                    break;
                case PathType.Row:
                    break;
                default:
                    ThrowTerminatingInvalidPathException(path);
                    break;
            }
        }

        /// <summary> 
        /// Determines if the specified path has child items. 
        /// </summary> 
        /// <param name="path">The path to examine.</param> 
        /// <returns> 
        /// True if the specified path has child items. 
        /// </returns> 
        protected override bool HasChildItems(string path)
        {
            WriteVerbose(string.Format("HasChildItems: <- Path='{0}'", path));
            PathDescriptor pathDescriptor = new PathDescriptor(path);
            switch(pathDescriptor.PathType)
            {
                case PathType.Root:
                case PathType.Database:
                case PathType.Schema:
                case PathType.ObjectType:
                    return true;
                case PathType.Object:
                    return true;
            }
            return false;
        }

        #endregion Container Methods

        #region Navigation Methods


        /// <summary> 
        /// Determine if the path specified is that of a container. 
        /// </summary> 
        /// <param name="path">The path to check.</param> 
        /// <returns>True if the path specifies a container.</returns> 
        protected override bool IsItemContainer(string path)
        {
            WriteVerbose(string.Format("IsItemContainer: <- Path='{0}'", path));
            PathDescriptor pathDescriptor = new PathDescriptor(path);
            if (pathDescriptor.PathType == PathType.Row)
            {
                WriteVerbose("IsItemContainer: -> false");
                return false;
            }
            WriteVerbose("IsItemContainer: -> true");
            return true;
        }

        /// <summary> 
        /// Gets the name of the leaf element in the specified path.         
        /// </summary> 
        /// <param name="path"> 
        /// The full or partial provider specific path. 
        /// </param> 
        /// <returns> 
        /// The leaf element in the path. 
        /// </returns> 
        /*protected override string GetChildName(string path)
        {
            WriteVerbose(string.Format("GetChildName: <- Path='{0}'", path));

            path = path.Replace("*", string.Empty);

            PathDescriptor pathDescription = new PathDescriptor(path);

            switch (pathDescription.PathType)
            {
                case PathType.Database:
                    WriteVerbose(string.Format("GetChildName: -> {{Database}} Name='{0}'", path));
                    return path;
                case PathType.Schema:
                    WriteVerbose(string.Format("GetChildName: -> {{Schema}} Name='{0}'", pathDescription.SchemaName));
                    return pathDescription.SchemaName;
                case PathType.ObjectType:
                    WriteVerbose(string.Format("GetChildName: -> {{DatabaseObjectType}} Name='{0}'", pathDescription.DatabaseObjectType));
                    return pathDescription.DatabaseObjectType.ToString();
                case PathType.Object:
                    WriteVerbose(string.Format("GetChildName: -> {{Object}} Name='{0}'", pathDescription.ObjectPath[0]));
                    return pathDescription.ObjectPath[0];
                case PathType.Row:
                    /// WriteVerbose(string.Format("GetChildName: -> {{Row}} Name='{0}'", key));
                    /// return key;
                default:
                    ThrowTerminatingInvalidPathException(path);
                    break;
            }
            return null;
        }*/

        /// <summary> 
        /// Returns the parent portion of the path, removing the child  
        /// segment of the path.  
        /// </summary> 
        /// <param name="path"> 
        /// A full or partial provider specific path. The path may be to an 
        /// item that may or may not exist. 
        /// </param> 
        /// <param name="root"> 
        /// The fully qualified path to the root of a drive. This parameter 
        /// may be null or empty if a mounted drive is not in use for this 
        /// operation.  If this parameter is not null or empty the result 
        /// of the method should not be a path to a container that is a 
        /// parent or in a different tree than the root. 
        /// </param> 
        /// <returns>The parent portion of the path.</returns> 
        protected override string GetParentPath(string path, string root)
        {
            // If the root is specified then the path has to contain 
            // the root. If not nothing should be returned.
            WriteVerbose(string.Format("GetParentPath: <- Path='{0}', Root='{1}'", path, root));

            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return null;
            }

            if (path.Equals(di.GetRootDrive()))
            {
                return string.Empty;
            }

            path = path.Replace(di.GetRootDrive(), string.Empty);
            if (!string.IsNullOrEmpty(root))
            {
                if (!path.Contains(root))
                {
                    WriteVerbose(string.Format("GetParentPath: -> '{0}'", root));
                    return root;
                }
            }
            if (!path.Contains(DatabaseUtils.PATH_SEPARATOR))
            {
                WriteVerbose(string.Format("GetParentPath: -> '{0}'", di.GetRootDrive()));
                return di.GetRootDrive();
            }

            string result = path.Substring(0, path.LastIndexOf(DatabaseUtils.PATH_SEPARATOR, StringComparison.OrdinalIgnoreCase));
            WriteVerbose(string.Format("GetParentPath: -> '{0}'", result));
            return result;
        }


        /// <summary> 
        /// Normalizes the path so that it is a relative path to the  
        /// basePath that was passed. 
        /// </summary> 
        /// <param name="path"> 
        /// A fully qualified provider specific path to an item.  The item 
        /// should exist or the provider should write out an error. 
        /// </param> 
        /// <param name="basepath"> 
        /// The path that the return value should be relative to. 
        /// </param> 
        /// <returns> 
        /// A normalized path that is relative to the basePath that was 
        /// passed. The provider should parse the path parameter, normalize 
        /// the path, and then return the normalized path relative to the 
        /// basePath. 
        /// </returns> 
        protected override string NormalizeRelativePath(string path, string basepath)
        {
            WriteVerbose(string.Format("NormalizeRelativePath: <- Path='{0}', Basepath='{1}'", path, basepath));

            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;

            // Normalize the paths first. 
            string normalPath = DatabaseUtils.NormalizePath(path);
            normalPath = DatabaseUtils.RemoveDriveFromPath(normalPath, di.GetRootDrive());
            string normalBasePath = DatabaseUtils.NormalizePath(basepath);
            normalBasePath = DatabaseUtils.RemoveDriveFromPath(normalBasePath, di.GetRootDrive());

            if (string.IsNullOrEmpty(normalBasePath))
            {
                return normalPath;
            }
            else
            {
                if (!normalPath.Contains(normalBasePath))
                {
                    return null;
                }
                return normalPath.Substring(normalBasePath.Length + DatabaseUtils.PATH_SEPARATOR.Length);
            }
        }


        /// <summary> 
        /// Joins two strings with a provider specific path separator. 
        /// </summary> 
        /// <param name="parent"> 
        /// The parent segment of a path to be joined with the child. 
        /// </param> 
        /// <param name="child"> 
        /// The child segment of a path to be joined with the parent. 
        /// </param> 
        /// <returns> 
        /// A string that contains the parent and child segments of the path 
        /// joined by a path separator. 
        /// </returns> 
        protected override string MakePath(string parent, string child)
        {
            WriteVerbose(string.Format("MakePath: <- Parent='{0}', Child='{1}'", parent, child));
            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            string result;
            string normalParent = DatabaseUtils.NormalizePath(parent);

            if (normalParent.EndsWith(DatabaseUtils.PATH_SEPARATOR))
            {
                normalParent = normalParent.Remove(normalParent.Length - 1);
            }
            string normalChild = DatabaseUtils.NormalizePath(child);
            if (normalChild.StartsWith(DatabaseUtils.PATH_SEPARATOR))
            {
                normalChild = normalChild.Substring(1);
            }
            
            if (String.IsNullOrEmpty(normalParent))
            {
                if (String.IsNullOrEmpty(normalChild))
                {
                    result = String.Empty;
                }
                else
                {
                    result = normalChild;
                }
            }
            else
            {
                if (String.IsNullOrEmpty(normalChild))
                {
                    result = normalParent + DatabaseUtils.PATH_SEPARATOR;
                }
                else
                {
                    result = normalParent + DatabaseUtils.PATH_SEPARATOR + normalChild;
                }
            }
            WriteVerbose(string.Format("MakePath: -> {0}", result));
            return result;
        }

        #endregion Navigation Methods

        #region Provider Capabilities 

        /**protected override string[] ExpandPath(string path)
        {
            DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return null;
            }
            path = path.Replace("*", string.Empty);
            PathDescriptor pathDescriptor = new PathDescriptor(path);
            switch (pathDescriptor.PathType)
            {
                case PathType.Database:
                    return di.GetSchemasNames("").ToArray();
                case PathType.Schema:
                    return di.GetSchemasNames("^" + pathDescriptor.SchemaName + ".*").ToArray();
                case PathType.Object:
                    return di.GetTablesNames("^" + pathDescriptor.ObjectPath[0] + ".*").ToArray();
            }
            return null;
        }*/

        #endregion Provider Capabilities
        
        private void ThrowTerminatingInvalidPathException(string path)
        {
            throw new ArgumentException(string.Format("Path must represent either a table or a row : {0}", path));
        }
    }
}