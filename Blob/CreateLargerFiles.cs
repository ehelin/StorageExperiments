using Shared.dto.blob;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Text;

namespace Blob
{
    public class CreateLargerFiles
    {
        private BlobDataStorageCredentials sourceCredentials;
        private BlobDataStorageCredentials destinationCredentials;
        private string localPath;
        private string cloudPath;
        private string largeFilePath;
        private long curByteCnt = 0;
        private int LargeFileCtr = 1;

        public CreateLargerFiles(BlobDataStorageCredentials sourceCredentials, BlobDataStorageCredentials destinationCredentials, string localPath, string cloudPath)
        {
            this.sourceCredentials = sourceCredentials;
            this.destinationCredentials = destinationCredentials;
            this.localPath = localPath;
            this.cloudPath = cloudPath;

            this.largeFilePath = localPath + "LargeFiles\\";
        }

        public void CreateFiles()
        {
            CloudBlobContainer srcContainer = Shared.Utilities.GetBlobStorageContainer(sourceCredentials.azureConnectionString, sourceCredentials.azureContainerName, false);
            ClearDirectory(largeFilePath);

            long ctr = 0;
            foreach (IListBlobItem item in srcContainer.ListBlobs(null, false))
            {
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;
                    long filesize = DownloadFile(blob, localPath);

                    this.curByteCnt += filesize;

                    //http://www.catalystsecure.com/blog/2011/05/how-many-bytes-in-a-gigabyte-my-answer-might-surprise-you/
                    if (this.curByteCnt >= 1073741824)
                    {
                        string file = CreateOneFile(localPath);
                        UploadFile(file);
                        this.curByteCnt = 0;
                    }
                    ctr++;
                }
            }

            if (this.curByteCnt >= 0)
            {
                string file = CreateOneFile(localPath);
                UploadFile(file);
            }
        }

        private string CreateOneFile(string localPath)
        {
            string loadFile = "LargeFile" + LargeFileCtr.ToString() + ".json";
            StreamWriter sw = null;
            StreamReader sr = null;
            StringBuilder sb = new StringBuilder();
            string largeFilePathFile = largeFilePath + loadFile;

            try
            {
                sw = new StreamWriter(largeFilePathFile, true);

                foreach (string file in Directory.GetFiles(localPath))
                {
                    sb.Clear();

                    sr = new StreamReader(file);
                    sb.Append(sr.ReadToEnd());

                    sr.Close();
                    sr.Dispose();
                    sr = null;

                    sw.WriteLine(sb.ToString());
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                Shared.Utilities.CloseIoObjects(sr, sw);
            }

            ClearDirectory(localPath);
            LargeFileCtr++;

            return largeFilePathFile;
        }

        private long DownloadFile(CloudBlockBlob cbb, string localDestPath)
        {
            string downloadPath = localDestPath + cbb.Name + "_downloaded.json";

            using (var fileStream = System.IO.File.OpenWrite(downloadPath))
            {
                cbb.DownloadToStream(fileStream);
            }

            FileInfo fi = new FileInfo(downloadPath);
            long fileSize = fi.Length;

            return fileSize;
        }
        private void UploadFile(string file)
        {
            CloudBlobContainer destContainer = Shared.Utilities.GetBlobStorageContainer(destinationCredentials.azureConnectionString, destinationCredentials.azureContainerName, false);
            FileInfo fi = new FileInfo(file);
            CloudBlockBlob cbb = destContainer.GetBlockBlobReference(fi.Name);

            using (var fileStream = System.IO.File.OpenRead(file))
            {
                cbb.UploadFromStream(fileStream);
            }
        }

        private string GetDownloadBlobFileContents(string filePath)
        {
            string fileContents = string.Empty;
            StreamReader sr = null;

            try
            {
                sr = new StreamReader(filePath);
                fileContents = sr.ReadToEnd();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (sr != null)
                {
                    sr.Close();
                    sr.Dispose();
                    sr = null;
                }
            }

            return fileContents;
        }
        private void ClearDirectory(string path)
        {
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            string[] files = Directory.GetFiles(path);
            foreach (string file in files)
                File.Delete(file);
        }
    }
}
