using System;
using System.Collections.Generic;
using Shared.dto.threading;
using Shared.dto.blob;
using Shared;
using Shared.dto.source;
using Shared.dto.tablestorage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;

namespace TableStorage
{
    public class TableStorageMain : Shared.dto.Main
    {
        private bool DeleteFiles = false;
        private bool DeleteContainers = false;

        public TableStorageMain(string pAzureConnectionString,
                                string pAzureContainerName,
                                int pMaxThreads,
                                long sourceRecordTotal,
                                bool pDeleteAllFiles,
                                bool pDeleteContainers)
        {
            this.Credentials = new TableStorageDataStorageCredentials(pAzureConnectionString, pAzureContainerName);
            this.StorageType = Enumeration.StorageTypes.TableStorage;
            this.DataJobs = new List<ThreadJob>();
            this.ThreadsComplete = new List<ThreadCompletion>();
            this.testCompletion = new TableStorageTestCompletion();
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
            Setup();
            RunExample();
        }

        #region Setup

        private void SetRecordCountTotal()
        {
            Database db = new Database();
            this.TotalRecordCount = db.GetSourcRecordCount();
        }

        private void Setup()
        {
            if (this.DeleteFiles)
            {
                DeleteAllFiles();
            }
            else if (this.DeleteContainers)
            {
                DeleteAllContainers();
                System.Threading.Thread.Sleep(300000);                    //wait for all containers to be deleted (5 minutes)
            }
        }
        private void DeleteAllFiles()
        {
            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)Credentials;
            CloudTable table = Utilities.GetTableStorageContainer(false, tsc.azureConnectionString, tsc.azureContainerName);

            Console.WriteLine("Starting to delete table storage files for cleanup...");

            if (table.Exists())
            {        
                List<SourceRecordTableStorage> updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                                                          select update).ToList<SourceRecordTableStorage>();

                foreach (SourceRecordTableStorage wu in updates)
                    table.Execute(TableOperation.Delete(wu));
            }

            Console.WriteLine("Done deleting table storage files for cleanup!");
        }
        private void DeleteAllContainers()
        {
            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)Credentials;
            CloudTableClient client = Utilities.GetTableStorageClient(tsc.azureConnectionString);

            Console.WriteLine("Starting to delete blob containers for cleanup...");

            foreach (CloudTable table in client.ListTables())
                table.DeleteIfExists();

            Console.WriteLine("Done deleting blob containers for cleanup!");
        }

        #endregion
    }
}
