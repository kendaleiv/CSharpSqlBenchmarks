using BenchmarkDotNet.Running;

namespace CSharpSqlBenchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<Benchmarks>();
            //Debug();
        }

        private static void Debug()
        {
            var benchmarks = new Benchmarks();

            try
            {
                benchmarks.GlobalSetup();
                benchmarks.LocalDb_ExecuteScaler_Sync();
            }
            finally
            {
                benchmarks.GlobalCleanup();
            }
        }
    }
}
