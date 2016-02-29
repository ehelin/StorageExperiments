namespace Shared.dto.blob
{
    public class BlobDataStorageCredentials : DataStorageCredentials
    {
        public string azureConnectionString = string.Empty;
        public string azureContainerName = string.Empty;

        public BlobDataStorageCredentials(string pAzureConnectionString, string pAzureContainerName)
        {
            azureConnectionString = pAzureConnectionString;
            azureContainerName = pAzureContainerName;
            this.CredentialTypes = Enumeration.StorageTypes.Blob;
        }
    }
}
