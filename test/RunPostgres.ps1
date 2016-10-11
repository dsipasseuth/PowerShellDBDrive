#
# RunPostgres.ps1
#
[reflection.assembly]::loadFrom( 'bin\Debug\net462\PowerShellDBDrive.dll' ) | import-module

New-PSDrive -Name postgres -PSProvider DatabaseProvider -Root "" -Provider NPGSQL -ConnectionString "Server=localhost;Port=5432;Database=docker;User Id=docker;Password=docker;"