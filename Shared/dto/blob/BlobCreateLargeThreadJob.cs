using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Collections.Generic;
using System;

namespace Shared.dto.blob
{
    public class BlobCreateLargeThreadJob : Shared.dto.threading.CreateLargeThreadJob
    {
        public BlobCreateLargeThreadJob() { }

        public BlobCreateLargeThreadJob(string sourceDir, string destinationFile)
            : base(sourceDir, destinationFile)
        {
            this.sourceDir = sourceDir;
            this.destinationFile = destinationFile;
        }

        protected async override void CreateLargeFile(int threadId, string sourceDir, string destinationFile, bool singleLargeFile)
        {
            CreateHiveFile(threadId, sourceDir, destinationFile, singleLargeFile);
        }
        //TODO - refactor...getting a bit messy with the singleLargeFile
        protected async void CreateHiveFile(int threadId, string sourceDir, string destinationFile, bool singleLargeFile)
        {
            StreamWriter sw = null;
            StreamReader sr = null;
            string line = string.Empty;
            long ctr = 0;
            Console.WriteLine("Large File Creation Thread " + threadId.ToString() + " starting! " + DateTime.Now.ToString());

            try
            {
                destinationFile = destinationFile + "\\" + threadId.ToString() + "LargeOutputFile.json";
                sw = new StreamWriter(destinationFile, true);

                //http://stackoverflow.com/questions/7865159/retrieving-files-from-directory-that-contains-large-amount-of-files
                foreach (string file in Directory.EnumerateFiles(GetPath(sourceDir, singleLargeFile)))
                {
                    if (file.IndexOf(".txt") == -1)
                    {
                        if (singleLargeFile)
                        {
                            Console.WriteLine("Writing " + file);
                            sr = new StreamReader(file);
                            while (sr.Peek() != -1)
                            {
                                line = sr.ReadLine();
                                sw.WriteLine(line);
                                sw.Flush();
                            }
                        }
                        else
                        {
                            line = sr.ReadToEnd();
                            sw.WriteLine(line);
                        }

                        Utilities.CloseIoObjects(sr, null);

                        ctr++;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ThreadId: " + threadId.ToString() + ", Error: " + e.Message);
            }
            finally
            {
                Utilities.CloseIoObjects(sr, sw);
            }

            Console.WriteLine("Large File Creation Thread " + threadId.ToString() + " Complete! " + DateTime.Now.ToString());
        }
        private string GetPath(string sourceDir, bool singleLargeFile)
        {
            string path = string.Empty;

            FileInfo fi = new FileInfo(sourceDir);
            if (singleLargeFile)
                path = sourceDir;
            else
                path = fi.DirectoryName;

            return path;
        }
    }
}
