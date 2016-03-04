using System;
using System.Collections.Generic;
using Shared.dto.threading;
using Shared.dto.blob;
using Shared.dto.tablestorage;
using Shared.dto.source;
using Shared.dto.documentdb;

namespace Shared.dto
{
    public class Main
    {
        public List<ThreadCompletion> ThreadsComplete { get; set; }
        protected TestComplete testCompletion = null;                   //handles end actions of whatever test is being run
        protected DataStorageCredentials Credentials { get; set; }
        protected Enumeration.StorageTypes StorageType;
        protected List<ThreadJob> DataJobs = null;
        protected bool LoadComplete = false;
        protected long MaxThreadsAllowed = 0;
        protected long TotalRecordCount = 0;
        protected long ThreadRecordCount = 0;

        protected virtual void RunExample()
        {
            long recordsPerThread = TotalRecordCount / MaxThreadsAllowed;
            int threadCnt = 1;
            long curStartId = 1;
            long curEndId = recordsPerThread;

            while (threadCnt <= MaxThreadsAllowed)
            {
                if (threadCnt > 1)
                {
                    curStartId = curStartId + recordsPerThread;
                    curEndId = curEndId + recordsPerThread;
                }

                ThreadCompletion tc = new ThreadCompletion();
                ThreadJob tj = GetThreadJob(tc, threadCnt, recordsPerThread, curStartId);
                this.DataJobs.Add(tj);
                this.ThreadsComplete.Add(tc);

                threadCnt++;
            }

            foreach (ThreadJob tj in DataJobs)
                tj.Execute();

            HandleExit();
        }



        protected void HandleExit()
        {
            //HACK:  System exit call made when all threads are done in the thread completion event
            while (true)
            {
                bool doneYet = true;

                foreach (ThreadCompletion tc in this.ThreadsComplete)
                {
                    if (!tc.ThreadDone)
                    {
                        doneYet = false;
                        break;
                    }
                }

                if (doneYet)
                {
                    LoadComplete = true;
                    break;
                }
                else
                    System.Windows.Forms.Application.DoEvents(); //helps?
            }

            testCompletion.HandleTestComplete(this.Credentials);
        }

        private ThreadJob GetThreadJob(ThreadCompletion tc, int threadCnt, long recordCount, long startId)
        {
            if (this.StorageType == Shared.Enumeration.StorageTypes.Blob)
                return new BlobLoadThreadJob(tc, this.Credentials, recordCount, startId, threadCnt);

            else if (this.StorageType == Shared.Enumeration.StorageTypes.EventHub)
                return new EventHub.EventHubLoadThreadJob(tc, this.Credentials, recordCount, startId, threadCnt);

            else if (this.StorageType == Enumeration.StorageTypes.SqlServer)
                return new SqlServer.SqlServerLoadThreadJob(tc, this.Credentials, recordCount, startId, threadCnt);

            else if (this.StorageType == Enumeration.StorageTypes.TableStorage)
                return new TableStorageLoadThreadJob(tc, this.Credentials, recordCount, startId, threadCnt);

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
