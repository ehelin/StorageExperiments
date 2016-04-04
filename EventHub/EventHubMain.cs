using Shared.dto.EventHub;
using Shared;
using System;
using Shared.dto;
using Shared;
using Shared.dto.threading;
using Shared.dto.SqlServer;

namespace EventHub
{
    public class EventHub : Shared.dto.Main
    {
        private SqlServerStorageCredentials streamAnalyticsDbCred = null;

        public EventHub(string conn)
        {
            streamAnalyticsDbCred = new SqlServerStorageCredentials(conn);
        }

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

        public void RunQueries(string dbConn)
        {
            ThreadJob tj = new EventHubLoadThreadJob();
            tj.RunCountQueries(streamAnalyticsDbCred);
        }
    }
}
