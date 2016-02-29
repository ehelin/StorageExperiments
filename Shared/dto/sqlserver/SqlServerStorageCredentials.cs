namespace Shared.dto.SqlServer
{
    public class SqlServerStorageCredentials : DataStorageCredentials
    {
        public string dbConnection = string.Empty;

        public SqlServerStorageCredentials(string pDbConnection)
        {
            dbConnection = pDbConnection;
            this.CredentialTypes = Enumeration.StorageTypes.SqlServer;
        }
    }
}
