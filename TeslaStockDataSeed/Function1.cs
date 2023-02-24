using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace TeslaStockDataSeed
{

    public class Function1
    {
        [FunctionName("Function1")]
        public void Run([QueueTrigger("teslaqueue", Connection = "TeslaStockConnection")] string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
            Console.WriteLine("finished");

            string filePath = "C:\\angular practice\\Tesla2021.csv";
            
            var configBuilder = new ConfigurationBuilder()
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables();
            var configuration = configBuilder.Build();


            string storageConnectionString = configuration.GetConnectionString("TeslaStockConnection");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("TeslaStockData");

            TableQuery<DynamicTableEntity> query = new TableQuery<DynamicTableEntity>();
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<DynamicTableEntity> queryResult = table.ExecuteQuerySegmented(query, continuationToken);
                continuationToken = queryResult.ContinuationToken;
                TableBatchOperation batchDeleteOperation = new TableBatchOperation();
                foreach (DynamicTableEntity entity in queryResult.Results)
                {
                    batchDeleteOperation.Delete(entity);
                    if (batchDeleteOperation.Count == 100)
                    {
                        table.ExecuteBatch(batchDeleteOperation);
                        batchDeleteOperation.Clear();
                    }
                }
                if (batchDeleteOperation.Count > 0)
                {
                    table.ExecuteBatch(batchDeleteOperation);
                }
            } while (continuationToken != null);

            Console.WriteLine("All data deleted successfully.");


            string[] lines = File.ReadAllLines(filePath);
            int i = 1000;

            foreach (string line in lines.Skip(1))
            {

                var values = line.Split(',');

                TeslaModel teslaModel = new TeslaModel
                {
                    PartitionKey = "partition key",
                    RowKey = i.ToString(),
                    Date = DateTime.Parse(values[0]),
                    Open = float.Parse(values[1]),
                    High = float.Parse(values[2]),
                    Low = float.Parse(values[3]),
                    Close = float.Parse(values[4]),
                    Adj_Close = float.Parse(values[5]),
                    Volume = int.Parse(values[6]),
                };

                i++;
                TableOperation replace = TableOperation.Insert(teslaModel);

                var res = table.Execute(replace);
            }
        }
    }

    public class TeslaModel : TableEntity
    {
        public int Id { get; set; }
        public DateTime Date { get; set; }
        public float Open { get; set; }
        public float High { get; set; }
        public float Low { get; set; }
        public float Close { get; set; }
        public float Adj_Close { get; set; }
        public int Volume { get; set; }

    }

}
