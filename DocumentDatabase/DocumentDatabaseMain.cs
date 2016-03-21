using Shared.dto;
using Shared;
using Shared.dto.documentdb;
using Shared.dto.threading;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Linq;
using System.Linq;
using System;

namespace DocumentDatabase
{
    public class DocumentDatabaseMain : Main
    {
        private bool DeleteFiles = false;
        private bool DeleteContainers = false;
        private string docDbId = string.Empty;
        private string docColId = string.Empty;

        public DocumentDatabaseMain(string pUrl,
                                string pKey,
                                int pMaxThreads,
                                long sourceRecordTotal,
                                bool pDeleteAllFiles,
                                bool pDeleteContainers)
        {
            this.Credentials = new DocumentDbDataStorageCredentials(pUrl, pKey);
            this.StorageType = Enumeration.StorageTypes.DocumentDb;
            this.MaxThreadsAllowed = pMaxThreads;

            if (sourceRecordTotal > 0)
                this.TotalRecordCount = sourceRecordTotal;
            else
                SetRecordCountTotal();

            this.DeleteFiles = pDeleteAllFiles;
            this.DeleteContainers = pDeleteContainers;
        }

        public void Run()
        {
            RunExample();
        }

        //NOTE - Document db implementation not really tested.
        protected override void RunExample()
        {
            List<Task> tasks = new List<Task>();
            long recordsPerThread = TotalRecordCount / MaxThreadsAllowed;
            int threadCnt = 1;
            long curStartId = 1;
            long curEndId = recordsPerThread;

            DocumentDbDataStorageCredentials dddss = (DocumentDbDataStorageCredentials)Credentials;
            DocumentClient dc = Utilities.GetDocumentDbClient(dddss.url, dddss.key);
            Database docDb = GetDatabase(dc).Result;

            //ThreadJob tj = GetThreadJob(threadCnt, recordsPerThread, curStartId);
            ThreadJob tj = new DocumentDbLoadThreadJob(this.Credentials, recordsPerThread, curStartId, threadCnt, docDb);
            tj.LaunchThreads();

            Console.Read();    //wait for developer to close application  
        }

        private async Task<Database> GetDatabase(DocumentClient client)
        {
            IEnumerable<Database> query = from db in client.CreateDatabaseQuery()
                                          where db.Id == DocumentDbConstants.DOCUMENT_DB_NAME
                                          select db;

            Database database = query.FirstOrDefault();
            if (database != null)
            {
                var result = await client.DeleteDatabaseAsync(database.SelfLink);
            }

            database = await client.CreateDatabaseAsync(new Database { Id = DocumentDbConstants.DOCUMENT_DB_NAME });

            return database;
        }
    }
}
