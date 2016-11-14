namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseColumnInfo : IDatabaseColumnInfo
    {
        public string ColumnName { get; set; }

        public bool Nillable { get { return IsNullable; } set { IsNullable = value; } } 

        public string SchemaName { get { return TableSchema; } set { TableSchema = value; } }

        public string TableName { get; set; }

		public string TableCatalog 				{get; set;}
        public string TableSchema             	{get; set;}
        public int OrdinalPosition         	    {get; set;}
        public string ColumnDefault           	{get; set;}
        public bool IsNullable              	{get; set;}
        public string DataType                	{get; set;}
        public int? CharacterMaximumLength  	{get; set;}
        public int? CharacterOctetLength    	{get; set;}
        public int? NumericPrecision        	{get; set;}
        public int? NumericPrecisionRadix   	{get; set;}
        public int? NumericScale            	{get; set;}
        public int? DatetimePrecision       	{get; set;}
        public string IntervalType            	{get; set;}
        public string CollationCatalog       	{get; set;}
        public string CollationSchema         	{get; set;}
        public string CollationName           	{get; set;}
        public string DomainCatalog           	{get; set;}
        public string DomainSchema            	{get; set;}
        public string DomainName              	{get; set;}
        public string UdtCatalog              	{get; set;}
        public string UdtSchema               	{get; set;}
        public string UdtName                 	{get; set;}
        public int? MaximumCardinality      	{get; set;}
        public string DtdIdentifier           	{get; set;}
        public bool IsUpdatable             	{get; set;}
    }
}
