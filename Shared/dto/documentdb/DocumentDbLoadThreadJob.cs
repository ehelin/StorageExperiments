using System;
using Shared.dto.threading;
using System.ComponentModel;
using Shared.dto.source;
using Microsoft.Azure.Documents.Client;
using System.Data.SqlClient;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Azure.Documents.Linq;

namespace Shared.dto.documentdb
{
    //https://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/ 
    public class DocumentDbLoadThreadJob : ThreadJob
    {
        public DocumentDbLoadThreadJob() : base() { }

        public DocumentDbLoadThreadJob(ThreadCompletion pDone,
                                 DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId) : base(pDone, pCredentials, recordCount, startId, threadId)
        {}

        public override async void DoWork(object sender, DoWorkEventArgs e)
        {
            DocumentDbDataStorageCredentials dddsc = (DocumentDbDataStorageCredentials)Credentials;
            DocumentClient dc = Utilities.GetDocumentDbClient(dddsc.url, dddsc.key);
            Microsoft.Azure.Documents.Database docDb = GetDatabase(dc).Result;
            DocumentCollection docCol = GetCollection(dc, docDb).Result;
            int ctr = 0;

            Console.WriteLine("Thread " + threadId + " starting with " + this.recordCount.ToString() + " records! " + DateTime.Now.ToString());

            while (ctr <= this.recordCount)
            {
                SourceRecord sr = GetRecord(startId);
                Update u = this.Parse(sr);

                if (u != null)
                {
                    bool created = InsertDocument(u, dc, docDb.Id, docCol.Id).Result;
                }

                startId++;
                ctr++;
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }

        private async Task<bool> InsertDocument(Update u, DocumentClient dc, string docDbId, string docColId)
        {
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
            catch (Exception e)
            {
                throw e;
            }

            return created;
        }

        //http://www.drdobbs.com/cloud/azure-documentdb-working-with-microsofts/240168992
        private async Task<DocumentCollection> GetCollection(DocumentClient dc, Microsoft.Azure.Documents.Database db)
        {
            DocumentCollection col = null;

            try
            {
                col = dc.CreateDocumentCollectionQuery(db.SelfLink).Where(c => c.Id == DocumentDbConstants.DOCUMENT_DB_COLLECTION_NAME).ToArray().FirstOrDefault();
                
                if (col == null)
                {
                    col = await dc.CreateDocumentCollectionAsync(db.SelfLink, new DocumentCollection { Id = DocumentDbConstants.DOCUMENT_DB_COLLECTION_NAME });
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            return col;
        }
        private async Task<Microsoft.Azure.Documents.Database> GetDatabase(Microsoft.Azure.Documents.Client.DocumentClient client)
        {
            IEnumerable<Microsoft.Azure.Documents.Database> query = from db in client.CreateDatabaseQuery()
                                          where db.Id == DocumentDbConstants.DOCUMENT_DB_NAME
                                          select db;

            Microsoft.Azure.Documents.Database database = query.FirstOrDefault();
            if (database == null)
            {
                database = await client.CreateDatabaseAsync(new Microsoft.Azure.Documents.Database { Id = DocumentDbConstants.DOCUMENT_DB_NAME });
            }

            return database;
        }
        private SourceRecord GetRecord(long id)
        {
            SqlConnection conn = null;
            SqlCommand cmd = null;
            SqlDataReader rdr = null;
            SourceRecord record = null;
            bool cont = true;
            int tryCtr = 0;

            while (cont)
            {
                try
                {
                    conn = new SqlConnection(SourceDataConstants.DB_CONNECTION);
                    cmd = conn.CreateCommand();
                    cmd.CommandTimeout = SourceDataConstants.DB_TIMEOUT;
                    cmd.CommandText = SourceDataConstants.SQL_GET_RECORD_ID;
                    cmd.CommandType = System.Data.CommandType.Text;

                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@id", id));

                    cmd.Connection.Open();

                    rdr = cmd.ExecuteReader();

                    if (rdr.Read())
                    {
                        record = new SourceRecord();

                        record.Id = Utilities.GetSafeInt(rdr[0]);
                        record.Type = Utilities.GetSafeString(rdr[1]);
                        record.Data = Utilities.GetSafeString(rdr[2]);
                        record.Created = Utilities.GetSafeDate(rdr[3]);
                    }

                    cont = false;
                }
                catch (Exception ex)
                {
                    if (tryCtr > 3)
                    {
                        throw ex;
                    }
                    else
                        tryCtr++;
                }
                finally
                {
                    Utilities.CloseDbObjects(conn, cmd, rdr, null);
                }
            }

            return record;
        }
    }
}


