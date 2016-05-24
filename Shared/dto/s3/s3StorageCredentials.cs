namespace Shared.dto.s3
{
    public class s3StorageCredentials : DataStorageCredentials
    {
        public string accessKey = string.Empty;
        public string secretKey = string.Empty;

        public s3StorageCredentials(string accessKey, string secretKey)
        {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.CredentialTypes = Enumeration.StorageTypes.DynamoDb;
        }
    }
}