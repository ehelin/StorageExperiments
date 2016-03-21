using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Shared
{
    public class SimpleThreadAsyncAwaitExample
    {
        public static string clsValue = string.Empty;

        public async Task<string> GetStringAndWait()
        {
            Console.WriteLine("Starting thread...");

            string value = await GetValueString("1");

            return value;
        }

        public async void GetStringAndNoWait(bool multipleThreads, bool useTaskEnd)
        {
            Console.WriteLine("Starting thread...");

            clsValue = string.Empty;

            List<Task> tasks = new List<Task>();

            if (!multipleThreads)
            {
                tasks.Add(Task.Run(() => GetValueString("1")));
            }
            else
            {
                tasks.Add(Task.Run(() => GetValueString("1")));
                tasks.Add(Task.Run(() => GetValueString("2")));
                tasks.Add(Task.Run(() => GetValueString("3")));
                tasks.Add(Task.Run(() => GetValueString("4")));
                tasks.Add(Task.Run(() => GetValueString("5")));

                if (useTaskEnd)
                {
                    await Task.WhenAll(tasks);
                    int waitForIt = 1;
                }
            }
        }

        private async Task<string> GetValueString(string threadId)
        {
            string value = string.Empty;
            int ctr = 0;

            Console.WriteLine("Starting thread " + threadId + "!");

            while (ctr < 10)
            {
                ctr++;
                System.Threading.Thread.Sleep(1000);
                Console.WriteLine("Thread " + threadId + " working: " + ctr.ToString());
            }

            value = ctr.ToString();
            clsValue = value;

            return value;
        }
    }
}
