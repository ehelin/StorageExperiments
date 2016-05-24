using Shared.dto.blob;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using Shared;
using System.Linq;
using System.Threading.Tasks;
using Shared.dto.threading;

namespace Blob
{
    public class CreateLargerFiles
    {
        private BlobDataStorageCredentials sourceCredentials;
        private BlobDataStorageCredentials destinationCredentials;
        private string localPath;
        private string cloudPath;
        private string localPathFileName;
        private string largeFilePath;
        private long curByteCnt = 0;
        private int LargeFileCtr = 1;

        public CreateLargerFiles(BlobDataStorageCredentials sourceCredentials,
                                 BlobDataStorageCredentials destinationCredentials,
                                 string localPath,
                                 string cloudPath,
                                 string localPathFileName)
        {
            this.sourceCredentials = sourceCredentials;
            this.destinationCredentials = destinationCredentials;
            this.localPath = localPath;
            this.cloudPath = cloudPath;
            this.localPathFileName = localPathFileName;

            this.largeFilePath = localPath + "LargeFiles\\";
        }

        public void WriteBlobFileNames()
        {
            CloudBlobContainer srcContainer = Shared.Utilities.GetBlobStorageContainer(sourceCredentials.azureConnectionString, sourceCredentials.azureContainerName, false);
            StreamWriter sw = null;

            WriteBlobFileNamesSetup();

            long ctr = 0;
            try
            {
                sw = new StreamWriter(this.localPathFileName, true);

                foreach (IListBlobItem item in srcContainer.ListBlobs(null, false))
                {
                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        CloudBlockBlob blob = (CloudBlockBlob)item;
                        sw.WriteLine(blob.Name);

                        if (ctr % 100000 == 0)
                            Console.WriteLine("Count: " + ctr.ToString() + "-" + DateTime.Now.ToString());

                        ctr++;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
            finally
            {
                Utilities.CloseIoObjects(null, sw);
            }

            Console.WriteLine("Writing blob file names done! " + DateTime.Now.ToString());
        }
        public void SeperateFileNamesIntoDirectories()
        {
            Dictionary<string, Dictionary<string, int>> threadPaths = PopulateDownloadThreadPaths();
            StreamReader sr = null;

            try
            {
                sr = new StreamReader(localPathFileName);
                string lastThread = string.Empty;

                long ctr = 0;
                while (sr.Peek() != -1)
                {
                    string line = sr.ReadLine();
                    WriteEntry(line, threadPaths);

                    if (ctr % 100000 == 0)
                    {
                        ListCounts(threadPaths);
                    }

                    ctr++;
                }

                Console.WriteLine("Done!");
            }
            catch (Exception e)
            {
                Console.WriteLine("Blob Download Error: " + e.Message);
            }
            finally
            {
                Utilities.CloseIoObjects(sr, null);
            }

            Console.Read();
        }


        public void DownloadFilesIntoDirectories()
        {
            List<string> directoryFileNames = GetDirectoryFileNames();
            DownloadThreadJob dtj = new BlobDownloadThreadJob(this.sourceCredentials, directoryFileNames);
            dtj.LaunchThreads();
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


        #region Private methods

        private List<string> GetDirectoryFileNames()
        {
            List<string> directoryFileNames = new List<string>();

            directoryFileNames.Add(this.localPath + "\\EastStatusUpdate\\EastStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\WestStatusUpdate\\WestStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\NorthStatusUpdate\\NorthStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\SouthStatusUpdate\\SouthStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\NorthEastStatusUpdate\\NorthEastStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\NorthWestStatusUpdate\\NorthWestStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\SouthWestStatusUpdate\\SouthWestStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\SouthEastStatusUpdate\\SouthEastStatusUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\EastClientUpdate\\EastClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\WestClientUpdate\\WestClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\NorthClientUpdate\\NorthClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\SouthClientUpdate\\SouthClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\NorthEastClientUpdate\\NorthEastClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\NorthWestClientUpdate\\NorthWestClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\SouthWestClientUpdate\\SouthWestClientUpdateFiles.txt");
            directoryFileNames.Add(this.localPath + "\\SouthEastClientUpdate\\SouthEastClientUpdateFiles.txt");

            return directoryFileNames;
        }
        private void WriteFileEntry(string line, Dictionary<string, Dictionary<string, int>> threadPaths, string key)
        {
            StreamWriter sw = null;

            try
            {
                Dictionary<string, int> threadPath = threadPaths[key];
                string path = threadPath.Keys.ToList()[0];
                threadPath[path]++;

                sw = new StreamWriter(path, true);
                sw.WriteLine(line);
            }
            catch (Exception e)
            {
                Console.WriteLine("File Write Error: " + e.Message);
            }
            finally
            {
                Utilities.CloseIoObjects(null, sw);
            }
        }
        private void WriteEntry(string line, Dictionary<string, Dictionary<string, int>> threadPaths)
        {
            if (line.IndexOf("_East_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_East_StatusUpdate_");
            else if (line.IndexOf("_West_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_West_StatusUpdate_");
            else if (line.IndexOf("_North_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_North_StatusUpdate_");
            else if (line.IndexOf("_South_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_South_StatusUpdate_");
            else if (line.IndexOf("_NorthEast_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_NorthEast_StatusUpdate_");
            else if (line.IndexOf("_NorthWest_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_NorthWest_StatusUpdate_");
            else if (line.IndexOf("_SouthWest_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_SouthWest_StatusUpdate_");
            else if (line.IndexOf("_SouthEast_StatusUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_SouthEast_StatusUpdate_");
            else if (line.IndexOf("_East_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_East_ClientUpdate_");
            else if (line.IndexOf("_West_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_West_ClientUpdate_");
            else if (line.IndexOf("_North_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_North_ClientUpdate_");
            else if (line.IndexOf("_South_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_South_ClientUpdate_");
            else if (line.IndexOf("_NorthEast_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_NorthEast_ClientUpdate_");
            else if (line.IndexOf("_NorthWest_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_NorthWest_ClientUpdate_");
            else if (line.IndexOf("_SouthWest_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_SouthWest_ClientUpdate_");
            else if (line.IndexOf("_SouthEast_ClientUpdate_") != -1)
                WriteFileEntry(line, threadPaths, "_SouthEast_ClientUpdate_");
            else
                throw new Exception("unknown thread path");
        }
        private void ListCounts(Dictionary<string, Dictionary<string, int>> threadPaths)
        {
            Console.WriteLine("");
            Console.WriteLine("Totals: " + DateTime.Now.ToString());
            foreach (var threadPath in threadPaths)
            {
                var count = threadPath.Value.ToList()[0];
                Console.WriteLine(threadPath.Key + "-" + count.Value);
            }
        }
        private void WriteBlobFileNamesSetup()
        {
            Console.WriteLine("Starting to write blob file names - " + DateTime.Now.ToString());

            if (File.Exists(this.localPathFileName))
                File.Delete(this.localPathFileName);

            File.Create(this.localPathFileName).Close();
        }    
        private Dictionary<string, Dictionary<string, int>> Add(string key, string path, Dictionary<string, Dictionary<string, int>> threadPaths)
        {
            Dictionary<string, int> pathCount = new Dictionary<string, int>();
            pathCount.Add(path, 0);
            threadPaths.Add(key, pathCount);
            FileInfo fi = new FileInfo(path);

            if (!Directory.Exists(fi.DirectoryName))
                Directory.CreateDirectory(fi.DirectoryName);

            foreach (string file in Directory.GetFiles(fi.DirectoryName))
            {
                File.Delete(file);
            }

            File.Delete(path);
            File.Create(path).Close();

            return threadPaths;
        }
        private Dictionary<string, Dictionary<string, int>> PopulateDownloadThreadPaths()
        {
            Dictionary<string, Dictionary<string, int>> threadPaths = new Dictionary<string, Dictionary<string, int>>();

            //NOTE: Refactor to include getting filename paths from  GetDirectoryFileNames()
            threadPaths = Add("_East_StatusUpdate_", this.localPath + "\\EastStatusUpdate\\EastStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_West_StatusUpdate_", this.localPath + "\\WestStatusUpdate\\WestStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_North_StatusUpdate_", this.localPath + "\\NorthStatusUpdate\\NorthStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_South_StatusUpdate_", this.localPath + "\\SouthStatusUpdate\\SouthStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_NorthEast_StatusUpdate_", this.localPath + "\\NorthEastStatusUpdate\\NorthEastStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_NorthWest_StatusUpdate_", this.localPath + "\\NorthWestStatusUpdate\\NorthWestStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_SouthWest_StatusUpdate_", this.localPath + "\\SouthWestStatusUpdate\\SouthWestStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_SouthEast_StatusUpdate_", this.localPath + "\\SouthEastStatusUpdate\\SouthEastStatusUpdateFiles.txt", threadPaths);
            threadPaths = Add("_East_ClientUpdate_", this.localPath + "\\EastClientUpdate\\EastClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_West_ClientUpdate_", this.localPath + "\\WestClientUpdate\\WestClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_North_ClientUpdate_", this.localPath + "\\NorthClientUpdate\\NorthClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_South_ClientUpdate_", this.localPath + "\\SouthClientUpdate\\SouthClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_NorthEast_ClientUpdate_", this.localPath + "\\NorthEastClientUpdate\\NorthEastClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_NorthWest_ClientUpdate_", this.localPath + "\\NorthWestClientUpdate\\NorthWestClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_SouthWest_ClientUpdate_", this.localPath + "\\SouthWestClientUpdate\\SouthWestClientUpdateFiles.txt", threadPaths);
            threadPaths = Add("_SouthEast_ClientUpdate_", this.localPath + "\\SouthEastClientUpdate\\SouthEastClientUpdateFiles.txt", threadPaths);

            return threadPaths;
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

        #endregion
    }
}
