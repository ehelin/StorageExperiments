using System;
using Shared.dto.threading;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

namespace Shared.dto.documentdb
{
    public class DocumentDbTestCompletion : TestComplete
    {
        public override void HandleTestComplete(DataStorageCredentials Credentials)
        {
            //TestComplete.RunUpdateCount();
            
            DocumentDbDataStorageCredentials dddsc = (DocumentDbDataStorageCredentials)Credentials;
            DocumentClient dc = Utilities.GetDocumentDbClient(dddsc.url, dddsc.key);
            var databaseCount = dc.CreateDatabaseQuery().ToList();
            Database azureDb = dc.CreateDatabaseQuery().Where(d => d.Id == DocumentDbConstants.DOCUMENT_DB_NAME).ToArray().FirstOrDefault();

            var collectionCount = dc.CreateDocumentCollectionQuery(azureDb.SelfLink).ToList();

            DocumentCollection update = dc.CreateDocumentCollectionQuery(azureDb.SelfLink).Where(c => c.Id == DocumentDbConstants.DOCUMENT_DB_COLLECTION_NAME).ToArray().FirstOrDefault();

            var documentCount = dc.CreateDocumentQuery(update.SelfLink, "SELECT * FROM c").ToList();

            Console.WriteLine("There are " + documentCount.Count().ToString() + " records!");
        }

        //TODO - can I have these methods in one place without breaking the threads?
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
    }
}
