using System;
using System.Collections.Generic;
using Shared.dto.source;
using Shared.dto.threading;
using Amazon;
using Amazon.S3;
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

        //NOTE:  A little different from the other implementations...seems better to count all at once looking for my specific queries in that general count
        public override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            DateTime start = DateTime.Now;
            bool specificRecordFnd = false;
            long specificTypeRecordCnt = 0;
            long recordCnt = 0;
            bool done = false;

            s3StorageCredentials credentials = (s3StorageCredentials)Credentials;
            AmazonS3Client client = new AmazonS3Client(credentials.accessKey, credentials.secretKey, RegionEndpoint.USWest2);
            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = Constants.S3_BUCKET_NAME;

            while (!done)
            {
                ListObjectsResponse response = client.ListObjects(request);
                foreach (S3Object o in response.S3Objects)
                {
                    if (o.Key.IndexOf(this.TestRecordId.ToString()) != -1)
                    {
                        specificRecordFnd = true;
                    }

                    if (o.Key.IndexOf(this.TestType) != -1)
                    {
                        specificTypeRecordCnt++;
                    }
                }

                recordCnt += response.S3Objects.Count;
                Console.WriteLine("Total Record Count: " + recordCnt.ToString() + " - " + DateTime.Now.ToString());

                request = AnotherMarker(response, request);
                if (request == null)
                {
                    done = true;
                    break;
                }
            }

            Console.WriteLine("Results: (start/end) - (" + start.ToString() + "/" + DateTime.Now.ToString() + ")");
            Console.WriteLine("Total Records: " + recordCnt.ToString());
            Console.WriteLine("Specific Record Found: " + specificRecordFnd.ToString());
            Console.WriteLine("Specific Record Type Found Count: " + specificTypeRecordCnt.ToString());
        }

        //Based heavily on this post - http://stackoverflow.com/questions/9920804/how-to-list-all-objects-in-amazon-s3-bucket
        private ListObjectsRequest AnotherMarker(ListObjectsResponse response, ListObjectsRequest request)
        {
            if (response.IsTruncated)
            {
                request.Marker = response.NextMarker;
            }
            else
            {
                request = null;
            }

            return request;
        }
    }
}
