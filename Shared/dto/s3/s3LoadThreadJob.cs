using System;
using System.Collections.Generic;
using Shared.dto.source;
using Shared.dto.threading;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon;
using Amazon.S3;
using Amazon.S3.IO;
using Amazon.S3.Model;

namespace Shared.dto.s3
{
    public class s3LoadThreadJob : ThreadJob
    {
        public s3LoadThreadJob() : base() { }

        public s3LoadThreadJob(DataStorageCredentials pCredentials,
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
            s3StorageCredentials cred = (s3StorageCredentials)Credentials;
            AmazonS3Client client = new AmazonS3Client(cred.accessKey, cred.secretKey, RegionEndpoint.USWest2);

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
        private void Upload(SourceRecord sr, AmazonS3Client client)
        {
            int errorTryCtr = 0;
            string name = "Thread" + this.threadId.ToString() + "_Db-" + sr.Id.ToString() + "_" + sr.Type.ToString();
            string data = Utilities.SerializeSourceRecord(sr);

            try
            {
                PutObjectRequest request = new PutObjectRequest();
                request.BucketName = Constants.S3_BUCKET_NAME;
                request.Key = name;
                request.ContentType = "text/plain";
                request.ContentBody = data;
                client.PutObject(request);
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
            s3StorageCredentials credentials = (s3StorageCredentials)Credentials;
            AmazonS3Client client = new AmazonS3Client(credentials.accessKey, credentials.secretKey, RegionEndpoint.USWest2);


            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = Constants.S3_BUCKET_NAME;
            ListObjectsResponse response = client.ListObjects(request);
            foreach (S3Object o in response.S3Objects)
            {
                recordCnt++;
            }

            Console.WriteLine("There were " + recordCnt.ToString() + " Inserted! " + DateTime.Now.ToString());
        }

        //private void GetSpecificId(DataStorageCredentials pCredentials)
        //{
        //    bool recordExists = false;
        //    BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)pCredentials;
        //    CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

        //    Console.WriteLine("Starting specific record search in blob storage for id " + this.TestRecordId.ToString() + " - " + DateTime.Now.ToString());

        //    foreach (IListBlobItem item in container.ListBlobs(null, false))
        //    {
        //        if (item.GetType() == typeof(CloudBlockBlob))
        //        {
        //            CloudBlockBlob blob = (CloudBlockBlob)item;

        //            //Thread1_Db-1000000_East_StatusUpdate_20164517064523768PM
        //            int start = blob.Name.IndexOf("-");
        //            string afterDb = blob.Name.Substring(start, blob.Name.Length - start);

        //            int end = afterDb.IndexOf("_");
        //            string dbIdStr = afterDb.Substring(0, end);
        //            dbIdStr = dbIdStr.Replace("-", "");
        //            dbIdStr = dbIdStr.Replace("_", "");

        //            //better test...is the id stored in the blob name?
        //            if (dbIdStr.Equals(this.TestRecordId.ToString()))
        //            {
        //                recordExists = true;
        //                break;
        //            }
        //        }
        //    }

        //    Console.WriteLine("Record exists (true/false): " + recordExists.ToString() + " - " + DateTime.Now.ToString());
        //}
        //private void GetCountForSpecificType(DataStorageCredentials pCredentials)
        //{
        //    long recordCnt = 0;
        //    BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)pCredentials;
        //    CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

        //    Console.WriteLine("Starting specific record search in blob storage for type " + this.TestType + " - " + DateTime.Now.ToString());

        //    foreach (IListBlobItem item in container.ListBlobs(null, false))
        //    {
        //        if (item.GetType() == typeof(CloudBlockBlob))
        //        {
        //            CloudBlockBlob blob = (CloudBlockBlob)item;

        //            if (blob.Name.IndexOf(this.TestType) != -1)
        //                recordCnt++;
        //        }
        //    }

        //    Console.WriteLine("There were " + recordCnt.ToString() + " matching the " + this.TestType + " type! " + DateTime.Now.ToString());
        //}
    }
}
