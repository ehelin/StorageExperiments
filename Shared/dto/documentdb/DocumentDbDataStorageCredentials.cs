using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared.dto.documentdb
{
    public class DocumentDbDataStorageCredentials : DataStorageCredentials
    {
        public string url = string.Empty;
        public string key = string.Empty;

        public DocumentDbDataStorageCredentials(string pUrl, string pKey)
        {
            url = pUrl;
            key = pKey;
            this.CredentialTypes = Enumeration.StorageTypes.DocumentDb;
        }
    }
}
