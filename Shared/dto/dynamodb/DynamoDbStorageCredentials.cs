namespace Shared.dto.dynamodb
{
    public class DynamoDbStorageCredentials : DataStorageCredentials
    {
        public string accessKey = string.Empty;
        public string secretKey = string.Empty;

        public DynamoDbStorageCredentials(string accessKey, string secretKey)
        {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.CredentialTypes = Enumeration.StorageTypes.DynamoDb;
        }
    }
}