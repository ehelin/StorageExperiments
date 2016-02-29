using System;
using System.Collections.Generic;
using Shared.dto.threading;
using Shared.dto.EventHub;
using Shared;
using Shared.dto.source;

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
            this.DataJobs = new List<ThreadJob>();
            this.ThreadsComplete = new List<ThreadCompletion>();
            this.testCompletion = new EventHubTestCompletion();
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
