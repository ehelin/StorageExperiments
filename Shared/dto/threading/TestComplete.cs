using System.Collections.Generic;
using System;
using Shared.dto.source;
using System.Linq;

namespace Shared.dto.threading
{
    public class TestComplete
    {
        public static IList<source.SourceRecord> updates = new List<source.SourceRecord>();

        public virtual void HandleTestComplete(DataStorageCredentials Credentials) { }

        public static void RunUpdateCount()
        {
            //----------- start test code for review ----------------------------------
            Console.WriteLine("");
            Console.WriteLine("----------------------");

            int updateCount = TestComplete.updates.Count;
            IEnumerable<SourceRecord> sortedEnum = TestComplete.updates.OrderBy(x => x.Id);

            using (var seq = sortedEnum.GetEnumerator())
            {
                while (seq.MoveNext())
                {
                    SourceRecord sr = (SourceRecord)seq.Current;
                    Console.WriteLine(sr.Id + "_" + sr.Type);
                }
            }
            //----------- end test code for review ----------------------------------
        }
    }
}
