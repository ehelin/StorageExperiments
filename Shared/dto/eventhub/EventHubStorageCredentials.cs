namespace Shared.dto.EventHub
{
    public class EventHubStorageCredentials : DataStorageCredentials
    {
        public string eventHub = string.Empty;
        public string eventHubName = string.Empty;

        public EventHubStorageCredentials(string pEventHub, string pEventHubName)
        {
            eventHub = pEventHub;
            eventHubName = pEventHubName;
            this.CredentialTypes = Enumeration.StorageTypes.EventHub;
        }
    }
}
