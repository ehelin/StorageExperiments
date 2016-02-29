namespace Shared.dto.tablestorage
{
    public class TableStorageDataStorageCredentials : DataStorageCredentials
    {
        public string azureConnectionString = string.Empty;
        public string azureContainerName = string.Empty;

        public TableStorageDataStorageCredentials(string pAzureConnectionString, string pAzureContainerName)
        {
            azureConnectionString = pAzureConnectionString;
            azureContainerName = pAzureContainerName;
            this.CredentialTypes = Enumeration.StorageTypes.TableStorage;
        }
    }
}
