# CSharpSqlBenchmarks

```
> dotnet build -c Release
> dotnet .\bin\Release\net5.0\CSharpSqlBenchmarks.dll
```

## Results

``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19042
Intel Core i7-1065G7 CPU 1.30GHz, 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=5.0.101
  [Host]     : .NET Core 5.0.1 (CoreCLR 5.0.120.57516, CoreFX 5.0.120.57516), X64 RyuJIT
  DefaultJob : .NET Core 5.0.1 (CoreCLR 5.0.120.57516, CoreFX 5.0.120.57516), X64 RyuJIT


```
|                                                                                 Method |          Mean |        Error |       StdDev |      Gen 0 |  Gen 1 | Gen 2 |    Allocated |
|--------------------------------------------------------------------------------------- |--------------:|-------------:|-------------:|-----------:|-------:|------:|-------------:|
|                                                             LocalDb_ExecuteScaler_Sync |      99.41 μs |     1.034 μs |     0.917 μs |     0.6104 | 0.1221 |     - |      2.72 KB |
|                                                  LocalDb_ExecuteScaler_AsyncConnection |      99.40 μs |     1.313 μs |     1.025 μs |     0.6104 | 0.1221 |     - |      2.95 KB |
|                             LocalDb_ExecuteScaler_AsyncConnectionAndAsyncExecuteScaler |     151.66 μs |     1.011 μs |     0.896 μs |     1.2207 |      - |     - |      5.14 KB |
|                                                          LocalDb_SqlDataReader_AllSync |      74.57 μs |     0.670 μs |     0.559 μs |     0.6104 | 0.1221 |     - |      2.79 KB |
|                                           LocalDb_SqlDataReader_AsyncConnection_OneRow |      71.29 μs |     0.612 μs |     0.511 μs |     0.7324 | 0.1221 |     - |      3.02 KB |
|                      LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader_OneRow |     113.33 μs |     1.427 μs |     1.191 μs |     1.0986 | 0.1221 |     - |      4.53 KB |
|          LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead_OneRow |     113.29 μs |     1.876 μs |     1.755 μs |     1.2207 |      - |     - |      5.13 KB |
|                                  LocalDb_SqlDataReader_AsyncConnection_OneThousandRows |     271.60 μs |     3.893 μs |     3.251 μs |    21.4844 |      - |     - |     88.91 KB |
|             LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader_OneThousandRows |     326.75 μs |     1.353 μs |     1.266 μs |    21.9727 |      - |     - |     90.43 KB |
| LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead_OneThousandRows |     400.97 μs |     2.214 μs |     1.849 μs |    36.6211 |      - |     - |    148.79 KB |
|                                   LocalDb_SqlDataReader_AsyncConnection_OneMillionRows | 162,186.81 μs | 2,322.878 μs | 2,172.821 μs | 21000.0000 |      - |     - |  85993.45 KB |
|              LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReader_OneMillionRows | 163,133.35 μs | 2,100.990 μs | 1,862.473 μs | 21000.0000 |      - |     - |   85994.9 KB |
|  LocalDb_SqlDataReader_AsyncConnectionAndAsyncExecuteReaderAndAsyncRead_OneMillionRows | 259,737.34 μs | 5,034.817 μs | 4,463.234 μs | 35000.0000 |      - |     - | 146159.24 KB |
