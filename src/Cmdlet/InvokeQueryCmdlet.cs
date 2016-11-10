using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Management.Automation;
using System.Linq;
using PowerShellDBDrive;

namespace PowerShellDBDrive.Cmdlets {
	/// <summary>
	///	Simple Cmdlet to allow arbitrary query on a database.
	/// </summary>
	[Cmdlet(VerbsLifecycle.Invoke,"Query")]
	[OutputType(typeof(PSObject))]
	public class InvokeQueryCmdlet : Cmdlet {

		[Parameter(Mandatory=true)]
		public string Provider {get; set;}

		[Parameter(Mandatory=true)]
		public string ConnectionString {get; set;}

		[Parameter(Mandatory=true)]
		public string Query {get ; set;}

		[Parameter]
		public Hashtable Parameters {get; set;} = new Hashtable();

		[Parameter]
		public int MaxResult {get; set;} = 0;

		[Parameter]
		public int Timeout {get; set;} = 60;

		private DbConnection CurrentConnection {get; set;}

		protected override void BeginProcessing() {
			base.BeginProcessing();
			DbProviderFactory factory = DbProviderFactories.GetFactory(Provider);
            DbConnectionStringBuilder csb = factory.CreateConnectionStringBuilder();
            csb.ConnectionString = ConnectionString;
			CurrentConnection = factory.CreateConnection();
            CurrentConnection.ConnectionString = csb.ConnectionString;
		}

		protected override void ProcessRecord() {
			using (BaseQueryManager bqm = new BaseQueryManager(CurrentConnection)) {
				long count = 1;
				IDictionary<string, object> namedParameters = Parameters.Cast<DictionaryEntry>().ToDictionary( kvp => (string) kvp.Key, kvp => (object) kvp.Value );
				foreach (PSObject p in bqm.QueryForObjects(Query, namedParameters, Timeout)) {
					WriteObject(p);
					if (count == MaxResult) {
						return;
					}
					count++;
				}
			}
		}

		protected override void EndProcessing() {
			if (CurrentConnection != null) {
				CurrentConnection.Dispose();	
			}
		}
	}
}