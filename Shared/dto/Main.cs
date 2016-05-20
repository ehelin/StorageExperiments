using System;
using System.Collections.Generic;
using Shared.dto.threading;
using Shared.dto.blob;
using Shared.dto.tablestorage;
using Shared.dto.source;
using Shared.dto.dynamodb;
using System.Threading.Tasks;

namespace Shared.dto
{
    public class Main
    {
        protected DataStorageCredentials Credentials { get; set; }
        protected Enumeration.StorageTypes StorageType;
        protected bool LoadComplete = false;
        protected long MaxThreadsAllowed = 0;
        protected long TotalRecordCount = 0;
        protected long ThreadRecordCount = 0;

        protected async virtual void RunExample()
        {
            List<Task> tasks = new List<Task>();
            long recordsPerThread = TotalRecordCount / MaxThreadsAllowed;
            int threadCnt = 1;
            long curStartId = 1;
            long curEndId = recordsPerThread;

            ThreadJob tj = GetThreadJob(threadCnt, recordsPerThread, curStartId);
            tj.LaunchThreads();
        }

        private ThreadJob GetThreadJob(int threadCnt, long recordCount, long startId)
        {
            if (this.StorageType == Shared.Enumeration.StorageTypes.Blob)
                return new BlobLoadThreadJob(this.Credentials, recordCount, startId, threadCnt);

            else if (this.StorageType == Shared.Enumeration.StorageTypes.EventHub)
                return new EventHub.EventHubLoadThreadJob(this.Credentials, recordCount, startId, threadCnt);

            else if (this.StorageType == Enumeration.StorageTypes.SqlServer)
                return new SqlServer.SqlServerLoadThreadJob(this.Credentials, recordCount, startId, threadCnt);

            else if (this.StorageType == Enumeration.StorageTypes.TableStorage)
                return new TableStorageLoadThreadJob(this.Credentials, recordCount, startId, threadCnt); 

            else if (this.StorageType == Enumeration.StorageTypes.DynamoDb)
                return new DynamoDbLoadThreadJob(this.Credentials, recordCount, startId, threadCnt);

            else
                throw new Exception("unknown thread job");
        }

        protected void SetRecordCountTotal()
        {
            Database db = new Database();
            this.TotalRecordCount = db.GetSourcRecordCount();
        }
    }
}
