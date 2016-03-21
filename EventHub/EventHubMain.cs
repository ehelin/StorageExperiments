using Shared.dto.EventHub;
using Shared;

namespace EventHub
{
    public class EventHub : Shared.dto.Main
    {
        public EventHub(string pAzureConnectionString,
                        string pAzureContainerName,
                        int pMaxThreads,
                        long sourceRecordTotal)
        {
            this.Credentials = new EventHubStorageCredentials(pAzureConnectionString, pAzureContainerName);
            this.StorageType = Enumeration.StorageTypes.EventHub;
            this.MaxThreadsAllowed = pMaxThreads;

            if (sourceRecordTotal > 0)
                this.TotalRecordCount = sourceRecordTotal;
            else
                SetRecordCountTotal();
        }

        public void Run()
        {
            RunExample();
        }
    }
}
