﻿using System;
using System.Data.SqlClient;
using Shared;
using Shared.dto.source;

namespace SourceData
{
    public class Database
    {
        public SourceRecord GetRecord(long id)
        {
            SqlConnection conn = null;
            SqlCommand cmd = null;
            SqlDataReader rdr = null;
            SourceRecord record = null;

            try
            {
                conn = new SqlConnection(SourceDataConstants.DB_CONNECTION);
                cmd = conn.CreateCommand();
                cmd.CommandTimeout = SourceDataConstants.DB_TIMEOUT;
                cmd.CommandText = SourceDataConstants.SQL_GET_RECORD_ID
                cmd.CommandType = System.Data.CommandType.StoredProcedure;

                cmd.Parameters.Add(new SqlParameter("@id", id));

                cmd.Connection.Open();

                rdr = cmd.ExecuteReader();
                
                if (rdr.Read())
                {
                    record = new SourceRecord();

                    record.Type = Utilities.GetSafeString(rdr[0]);
                    record.Data = Utilities.GetSafeString(rdr[1]);
                    record.Created = Utilities.GetSafeDate(rdr[2]);
                }
                
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                Utilities.CloseDbObjects(conn, cmd, rdr, null);
            }

            return record;
        }
        public long GetSourcRecordCount()
        {
            SqlConnection conn = null;
            SqlCommand cmd = null;
            string curFile = string.Empty;
            long recordCount = 0;

            try
            {
                conn = new SqlConnection(SourceDataConstants.DB_CONNECTION);
                cmd = new SqlCommand();
                cmd.CommandTimeout = SourceDataConstants.DB_TIMEOUT;
                cmd.Connection = conn;
                cmd.CommandText = SourceDataConstants.SQL_GET_RECORD_COUNT;
                cmd.CommandType = System.Data.CommandType.Text;

                conn.Open();

                recordCount = Convert.ToInt64(cmd.ExecuteScalar());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                Utilities.CloseDbObjects(conn, cmd, null, null);
            }

            return recordCount;
        }
    }
}
