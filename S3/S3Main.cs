using Shared;
using Shared.dto.s3;
using System;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace S3
{
    public class S3Main : Shared.dto.Main
    {
        public S3Main() { }

        public S3Main(string accessKey,
                            string secretKey,
                            int pMaxThreads,
                            long sourceRecordTotal)
        {
            this.Credentials = new s3StorageCredentials(accessKey, secretKey);
            this.StorageType = Enumeration.StorageTypes.S3;
            this.MaxThreadsAllowed = pMaxThreads;

            if (sourceRecordTotal > 0)
                this.TotalRecordCount = sourceRecordTotal;
            else
                SetRecordCountTotal();
        }

        public void Run()
        {
            s3StorageCredentials cred = (s3StorageCredentials)this.Credentials;
            AmazonS3Client client = new AmazonS3Client(cred.accessKey, cred.secretKey, RegionEndpoint.USWest2);

            Console.WriteLine("Setting up bucket...");
            //DeleteBucket(client);
            DeleteS3Records(client);
            //CreateBucket(client);           

            //Console.WriteLine("Done waiting...starting data load!");
            //RunExample();
        }

        private void CreateBucket(AmazonS3Client client)
        {
            if (!Amazon.S3.Util.AmazonS3Util.DoesS3BucketExist(client, Constants.S3_BUCKET_NAME))
            {
                try
                {
                    PutBucketRequest createRequest = new PutBucketRequest();
                    createRequest.BucketName = Constants.S3_BUCKET_NAME;
                    createRequest.UseClientRegion = true;
                    createRequest.CannedACL = S3CannedACL.PublicReadWrite;
                    createRequest.BucketRegion = S3Region.USW2;
                    client.PutBucket(createRequest);
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }
        private void DeleteBucket(AmazonS3Client client)
        {
            try
            {
                Amazon.S3.Util.AmazonS3Util.DeleteS3BucketWithObjects(client, Constants.S3_BUCKET_NAME);
            }
            catch (Exception e)
            {
                if (e.Message.IndexOf("The specified bucket does not exist") == -1)
                    throw e;
            }
        }

        public void DeleteS3Records(AmazonS3Client client)
        {
            bool done = false;
            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = Constants.S3_BUCKET_NAME;

            while (!done)
            {
                ListObjectsResponse response = client.ListObjects(request);
                foreach (S3Object o in response.S3Objects)
                {
                    DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest();
                    deleteObjectRequest.BucketName = Constants.S3_BUCKET_NAME;
                    deleteObjectRequest.Key = o.Key;

                    client.DeleteObject(deleteObjectRequest);
                }

                request = Utilities.AnotherMarker(response, request);
                if (request == null)
                {
                    done = true;
                    break;
                }
            }
        }
    }
}
