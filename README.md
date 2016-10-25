# PowerShellDBDrive
Database Providers for Powershell to mount SQL Database as a Drive

# Prerequisite
Easiest way is to install drivers into GAC either for Oracle or PostgreSQL.

# Building 

After cloning the project : 

```powershell
C:\PS> dotnet restore
C:\PS> dotnet build
```

# Usage
For now, it's not package as a PowerShell module, so you have to load the DLL.

```powershell
C:\PS> import-module .\PowerShellDBDrive.dll;
```

To mount an Oracle Database as a drive : 

```powershell
C:\PS> $ConnectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host.company.org)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SID=OracleSID)));User Id=USERNAME;Password=PASSWORD;";
C:\PS> New-PSDrive -Name oracledb -PSProvider DatabaseProvider -Root "" -Provider Oracle.ManagedDataAccess.Client -ConnectionString $ConnectionString -Verbose
```

For now, only read (with some limitation) is supported : 

```powershell
oracledb:\> ls 
... [should return all schema/user]
oracledb:\> cd schemaname
oracledb:\schemaname> ls
... [should return all tables of given schema]
oracledb:\schemaname> cd tablename
oracledb:\schemaname\tablename> ls
... [should return the 100 first lines of table]
```

Obviously, it's compatible with other PowerShell Commands. 

This could be more efficient if we could give Get-ChildItem via dynamic parameters the columns to retrieve.

Filter support for Get-ChildItem would also be a nice feature to be add.

```powershell
# This command should export the first 100 lines of tablename into a CSV using UTF-8 (with bom)
oracledb:\schemaname\tablename> ls | select -property Id,Name,SomeColumns | ConvertTo-Csv -NoTypeInformation | Out-File C:\Temp\Test.csv -Encoding UTF8
```

# Database Supported 

As of now, there are two target databases. (Could add more, but let's finish these first)

- Oracle (WIP)
- PostgreSQL (WIP)
