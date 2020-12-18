using BenchmarkDotNet.Attributes;
using MoreLinq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using ThrowawayDb;

namespace CSharpSqlBenchmarks
{
    [MemoryDiagnoser]
    public class Benchmarks
    {
        private ThrowawayDatabase db;

        [GlobalSetup]
        public void GlobalSetup()
        {
            db = ThrowawayDatabase.FromLocalInstance(@"(localdb)\MSSQLLocalDB");

            var databaseRows = new Dictionary<string, int>
            {
                ["OneRow"] = 1,
                ["OneThousandRows"] = 1_000,
                ["OneMillionRows"] = 1_000_000
            };

            using var connection = new SqlConnection(db.ConnectionString);
            connection.Open();

            foreach (var kvp in databaseRows)
            {
                using var cmd = new SqlCommand($"create table {kvp.Key} (id uniqueidentifier not null primary key)", connection);
                
                cmd.ExecuteNonQuery();

                var insertValues = Enumerable.Range(1, kvp.Value).Select(_ => $"('{Guid.NewGuid()}')");

                // Maximum values at a time is 1,000
                foreach (var batch in insertValues.Batch(1_000))
                {
                    using var insertCmd = new SqlCommand($"insert into {kvp.Key} values {string.Join(",", batch)}", connection);
                    insertCmd.ExecuteNonQuery();
                }
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            db.Dispose();
        }

        [Benchmark]
        public void LocalDb_ExecuteScaler_Sync()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            using var cmd = new SqlCommand("select 1", connection);

            connection.Open();

            var result = cmd.ExecuteScalar();
        }

        [Benchmark]
        public async Task LocalDb_ExecuteScaler_AsyncConnection()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            using var cmd = new SqlCommand("SELECT 1", connection);

            await connection.OpenAsync();

            var result = cmd.ExecuteScalar();
        }

        [Benchmark]
        public async Task LocalDb_ExecuteScaler_AsyncConnectionAndAsyncExecuteScaler()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            using var cmd = new SqlCommand("SELECT 1", connection);

            await connection.OpenAsync();

            var result = await cmd.ExecuteScalarAsync();
        }

        [Benchmark]
        public void LocalDb_SqlDataReader_AllSync()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            connection.Open();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneRow";
            var reader = selectCommand.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        //[Benchmark]
        //public async Task LocalDb_SqlDataReader_AsyncConnection()
        //{
        //    using var connection = new SqlConnection(db.ConnectionString);
        //    await connection.OpenAsync();

        //    var selectCommand = connection.CreateCommand();
        //    selectCommand.CommandText = "SELECT id from OneRow";
        //    var reader = selectCommand.ExecuteReader();

        //    if (reader.HasRows)
        //    {
        //        while (reader.Read())
        //        {
        //            reader.GetGuid(0);
        //        }
        //    }

        //    reader.Close();
        //}

        //[Benchmark]
        //public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader()
        //{
        //    using var connection = new SqlConnection(db.ConnectionString);
        //    await connection.OpenAsync();

        //    var selectCommand = connection.CreateCommand();
        //    selectCommand.CommandText = "SELECT id from OneRow";
        //    var reader = await selectCommand.ExecuteReaderAsync();

        //    if (reader.HasRows)
        //    {
        //        while (reader.Read())
        //        {
        //            reader.GetGuid(0);
        //        }
        //    }

        //    reader.Close();
        //}

        //[Benchmark]
        //public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead()
        //{
        //    using var connection = new SqlConnection(db.ConnectionString);
        //    await connection.OpenAsync();

        //    var selectCommand = connection.CreateCommand();
        //    selectCommand.CommandText = "SELECT id from OneRow";
        //    var reader = await selectCommand.ExecuteReaderAsync();

        //    if (reader.HasRows)
        //    {
        //        while (await reader.ReadAsync())
        //        {
        //            reader.GetGuid(0);
        //        }
        //    }

        //    reader.Close();
        //}

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnection_OneRow()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneRow";
            var reader = selectCommand.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader_OneRow()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneRow";
            var reader = await selectCommand.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead_OneRow()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneRow";
            var reader = await selectCommand.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                while (await reader.ReadAsync())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnection_OneThousandRows()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneThousandRows";
            var reader = selectCommand.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader_OneThousandRows()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneThousandRows";
            var reader = await selectCommand.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead_OneThousandRows()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneThousandRows";
            var reader = await selectCommand.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                while (await reader.ReadAsync())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnection_OneMillionRows()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneMillionRows";
            var reader = selectCommand.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader_OneMillionRows()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneMillionRows";
            var reader = await selectCommand.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }

        [Benchmark]
        public async Task LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead_OneMillionRows()
        {
            using var connection = new SqlConnection(db.ConnectionString);
            await connection.OpenAsync();

            var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT id from OneMillionRows";
            var reader = await selectCommand.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                while (await reader.ReadAsync())
                {
                    reader.GetGuid(0);
                }
            }

            reader.Close();
        }
    }
}
