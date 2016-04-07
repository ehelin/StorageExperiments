using System;
using Shared.dto.threading;
using Shared.dto.source;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Azure.Documents.Linq;
using System.Collections.Generic;

namespace Shared.dto.documentdb
{
    public class DocumentDbLoadThreadJob : ThreadJob
    {
        private Microsoft.Azure.Documents.Database docDb = null;

        public DocumentDbLoadThreadJob() : base() { }

        public DocumentDbLoadThreadJob(DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId,
                                 Microsoft.Azure.Documents.Database pDocDb)
            : base(pCredentials, recordCount, startId, threadId)
        {
            docDb = pDocDb;
        }

        //NOTE: I had hoped that all of the data loads would have used the same launchthread method in ThreadJob, but because Document db on the 
        //price tier I appear to have access too, the maximum thread count I have been able to run without throwing the 'Rate to large' error is 5. Ideally,
        //I will refactor this to be more elegant.
        public virtual async void LaunchThreads()
        {
            List<Task> tasks = new List<Task>();

            long thread1StartId = 1;
            long thread1EndId = thread1StartId + this.recordCount;

            long thread2StartId = thread1EndId + 1;
            long thread2EndId = thread2StartId + this.recordCount;

            long thread3StartId = thread2EndId + 1;
            long thread3EndId = thread3StartId + this.recordCount;

            long thread4StartId = thread3EndId + 1;
            long thread4EndId = thread4StartId + this.recordCount;

            long thread5StartId = thread4EndId + 1;
            long thread5EndId = thread5StartId + this.recordCount;

            tasks.Add(Task.Run(() => RunLoad(1, this.recordCount, this.Credentials, thread1StartId, thread1EndId)));
            tasks.Add(Task.Run(() => RunLoad(2, this.recordCount, this.Credentials, thread2StartId, thread2EndId)));
            tasks.Add(Task.Run(() => RunLoad(3, this.recordCount, this.Credentials, thread3StartId, thread3EndId)));
            tasks.Add(Task.Run(() => RunLoad(4, this.recordCount, this.Credentials, thread4StartId, thread4EndId)));
            tasks.Add(Task.Run(() => RunLoad(5, this.recordCount, this.Credentials, thread5StartId, thread5EndId)));

            await Task.WhenAll(tasks);

            RunCountQueries(this.Credentials);
        }

        protected async override void RunLoad(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            LoadRecords(pThreadId, pRecordCount, pCredentials, pStartId, pEndPoint);
        }

        private async void LoadRecords(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            Shared.dto.source.Database db = new Shared.dto.source.Database();
            DocumentDbDataStorageCredentials dddss = (DocumentDbDataStorageCredentials)Credentials;
            DocumentClient dc = Utilities.GetDocumentDbClient(dddss.url, dddss.key);
            DocumentCollection dCol = GetCollection(dc, docDb, this.threadId).Result;
            long threadId = pThreadId;
            long recordCount = pRecordCount;
            long startId = pStartId;
            long endId = pEndPoint;

            Console.WriteLine("Thread " + threadId + " (RcrdCnt/Start/End) (" + recordCount.ToString() + "/"
                                + startId.ToString() + "/" + endId.ToString() + ")" + DateTime.Now.ToString());

            while (startId <= endId)
            {
                try
                {
                    IList<SourceRecord> scrRecords = db.GetRecord(startId, endId);

                    foreach (SourceRecord sr in scrRecords)
                    {
                        Update u = this.Parse(sr);
                        bool created = InsertDocument(u, dc, docDb.Id, dCol.Id).Result;
                        System.Threading.Thread.Sleep(2000);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }

                startId = startId + Convert.ToInt64(SourceDataConstants.SQL_GET_TOP_VALUE);
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }
        
        private async Task<bool> InsertDocument(Update u, DocumentClient dc, string docDbId, string docColId)
        {
            int errorTryCtr = 0;
            bool created = false;

            try
            {
                if (u.GetType().Equals(typeof(source.Status)))
                {
                    Status s = (Status)u;
                    await dc.CreateDocumentAsync("dbs/" + docDbId + "/colls/" + docColId, s);
                }
                else
                {
                    SatelliteClient sc = (SatelliteClient)u;
                    await dc.CreateDocumentAsync("dbs/" + docDbId + "/colls/" + docColId, sc);
                }
            }
            catch (Exception ex)
            {
                if (errorTryCtr > Constants.MAX_RETRY)
                    Console.WriteLine("Document Database - Max error limit reached...moving on");
                else
                {
                    Console.WriteLine("Document Database ERROR: - " + ex.Message);
                    errorTryCtr++;
                }
            }

            return created;
        }
        private async Task<DocumentCollection> GetCollection(DocumentClient dc, Microsoft.Azure.Documents.Database db, long threadId)
        {
            DocumentCollection col = null;

            try
            {
                col = dc.CreateDocumentCollectionQuery(db.SelfLink).Where(c => c.Id == DocumentDbConstants.DOCUMENT_DB_COLLECTION_NAME + threadId.ToString()).ToArray().FirstOrDefault();

                if (col == null)
                {
                    col = await dc.CreateDocumentCollectionAsync(db.SelfLink, new DocumentCollection { Id = DocumentDbConstants.DOCUMENT_DB_COLLECTION_NAME + threadId.ToString() });
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            return col;
        }
        
        public override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            GetTotalRecordCount(pCredentials);
            GetSpecificId();
            GetCountForSpecificType();
        }

        protected void GetTotalRecordCount(DataStorageCredentials pCredentials)
        {
            Console.WriteLine("Starting total document db record count! " + DateTime.Now.ToString());

            DocumentDbDataStorageCredentials dddsc = (DocumentDbDataStorageCredentials)Credentials;
            DocumentClient dc = Utilities.GetDocumentDbClient(dddsc.url, dddsc.key);
            var databaseCount = dc.CreateDatabaseQuery().ToList();
            Microsoft.Azure.Documents.Database azureDb = dc.CreateDatabaseQuery().Where(d => d.Id == DocumentDbConstants.DOCUMENT_DB_NAME).ToArray().FirstOrDefault();

            var collectionCount = dc.CreateDocumentCollectionQuery(azureDb.SelfLink).ToList();

            DocumentCollection update = dc.CreateDocumentCollectionQuery(azureDb.SelfLink).Where(c => c.Id == DocumentDbConstants.DOCUMENT_DB_COLLECTION_NAME).ToArray().FirstOrDefault();

            var documentCount = dc.CreateDocumentQuery(update.SelfLink, "SELECT * FROM c").ToList();

            Console.WriteLine("There are " + documentCount.Count().ToString() + " records! " + DateTime.Now.ToString());
        }
        private void GetSpecificId()
        {
            throw new NotImplementedException();
        }
        private void GetCountForSpecificType()
        {
            throw new NotImplementedException();
        }
    }
}


