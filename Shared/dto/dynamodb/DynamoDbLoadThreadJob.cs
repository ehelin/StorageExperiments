using System;
using System.Collections.Generic;
using Shared.dto.source;
using Shared.dto.threading;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon;
using Amazon.DynamoDBv2.Model;

namespace Shared.dto.dynamodb
{
    public class DynamoDbLoadThreadJob : ThreadJob
    {
        public DynamoDbLoadThreadJob() : base() { }

        public DynamoDbLoadThreadJob(DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId)
            : base(pCredentials, recordCount, startId, threadId)
        { }

        protected async override void RunLoad(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            LoadRecords(pThreadId, pRecordCount, pCredentials, pStartId, pEndPoint);
        }

        private async void LoadRecords(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            Database db = new Database();
            DynamoDbStorageCredentials cred = (DynamoDbStorageCredentials)Credentials;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(cred.accessKey, cred.secretKey, RegionEndpoint.USWest2);
            long threadId = pThreadId;
            long recordCount = pRecordCount;
            long startId = pStartId;
            long endId = pEndPoint;
            int ctr = 0;
            bool done = false;

            Console.WriteLine("Thread " + threadId + " (RcrdCnt/Start/End) (" + recordCount.ToString() + "/"
                                + startId.ToString() + "/" + endId.ToString() + ")" + DateTime.Now.ToString());

            while (startId <= endId)
            {
                try
                {
                    IList<SourceRecord> scrRecords = db.GetRecord(startId, endId);

                    foreach (SourceRecord sr in scrRecords)
                    {
                        Upload(sr, client);

                        //if (ctr >= 1000)
                        //{
                        //    done = true;
                        //    break;
                        //}
                        //else
                        //{
                        //    ctr++;
                        //}
                    }

                    //if (done)
                    //    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }

                startId = startId + Convert.ToInt64(SourceDataConstants.SQL_GET_TOP_VALUE);
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }
        private void Upload(SourceRecord sr, AmazonDynamoDBClient client)
        {
            int errorTryCtr = 0;
            string name = "Thread" + this.threadId.ToString() + "_Db-" + sr.Id.ToString() + "_" + sr.Type.ToString();
            string data = Utilities.SerializeSourceRecord(sr);

            try
            {
                Table satelliteUpdates = Table.LoadTable(client, Constants.DYNAMO_DB_TABLE_NAME);

                Document satelliteUpdate = new Document();
                satelliteUpdate["SatelliteId"] = sr.Id;
                satelliteUpdate["SatelliteRange"] = name;

                satelliteUpdate["Id"] = sr.Id;
                satelliteUpdate["Type"] = sr.Type;
                satelliteUpdate["Data"] = data;
                satelliteUpdate["Created"] = sr.Created;

                satelliteUpdates.PutItem(satelliteUpdate);
            }
            catch (Exception e)
            {
                if (errorTryCtr > Constants.MAX_RETRY)
                    Console.WriteLine("Blob - Max error limit reached...moving on");
                else
                {
                    Console.WriteLine("Dynamo Db ERROR: - Thread" + this.threadId.ToString() + e.Message);
                    errorTryCtr++;
                }
            }
        }

        public override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            GetTotalRecordCount(pCredentials);
            //GetSpecificId(pCredentials);
            //GetCountForSpecificType(pCredentials);
        }
        private void GetTotalRecordCount(DataStorageCredentials pCredentials)
        {
            long recordCnt = 0;
            DynamoDbStorageCredentials credentials = (DynamoDbStorageCredentials)Credentials;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials.accessKey, credentials.secretKey, RegionEndpoint.USWest2);
            Table satelliteUpdates = Table.LoadTable(client, Constants.DYNAMO_DB_TABLE_NAME);
            ScanFilter sc = new ScanFilter();

            sc.AddCondition("SatelliteId", ScanOperator.GreaterThanOrEqual, 0);
            Search srch = satelliteUpdates.Scan(sc);

            List<Document> results = new List<Document>();
            do
            {
                results = srch.GetNextSet();
                foreach (var result in results)
                {
                    recordCnt++;
                }

            } while (!srch.IsDone);

            Console.WriteLine("There were " + recordCnt.ToString() + " Inserted! " + DateTime.Now.ToString());
        }

        private void GetSpecificId(DataStorageCredentials pCredentials)
        {
            throw new NotImplementedException();
        }
        private void GetCountForSpecificType(DataStorageCredentials pCredentials)
        {
            throw new NotImplementedException();
        }
    }
}
