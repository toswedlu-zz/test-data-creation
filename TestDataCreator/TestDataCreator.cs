using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TestDataCreator;

namespace CosmosDB
{
    public class TestDataCreator
    {
        CosmosClient _client;

        public TestDataCreator(string connectionString)
        {
            CosmosClientOptions options = new CosmosClientOptions() { AllowBulkExecution = true, RequestTimeout = new TimeSpan(0, 3, 0) };
            _client = new CosmosClient(connectionString, options);
        }

        public async Task<Container> CreateContainerAsync(string dbName, string containerName, string pkPath)
        {
            DatabaseResponse dbResp = await _client.CreateDatabaseIfNotExistsAsync(dbName);
            ContainerProperties props = new ContainerProperties(containerName, pkPath) { AnalyticalStoreTimeToLiveInSeconds = -1 };
            ContainerResponse contResp = await dbResp.Database.CreateContainerIfNotExistsAsync(props);
            return contResp.Container;
        }

        public async Task IngestDataNoSplit(Container container, int numPartitions = 3, int fieldLength = 100)
        {
            int numDocsPerPartition = 1000;
            ThroughputProperties props = ThroughputProperties.CreateAutoscaleThroughput(10000);
            await container.ReplaceThroughputAsync(props);
            Func<string, int, int, Stream> func = (id, p, f) => { return Utils.CreateRandomDocument(p, f, ref id); };
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, false);
        }

        public async Task IngestDataWithSplit(Container container, int numPartitions = 5, int fieldLength = 100)
        {
            Console.WriteLine("Starting IngestDataWithSplit.");
            int numDocsPerPartition = 500;
            Console.WriteLine("Ingesting data.");
            await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(numPartitions * 10000));
            await Utils.WaitForOfferReplaceAsync(container);
            Func<string, int, int, Stream> func = (id, p, f) => { return Utils.CreateRandomDocument(p, f, ref id); };
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, false);
        }       

        public async Task IngestDataArchivedPartitions(Container container, int numPartitions = 3, int fieldLength = 100)
        {
            int numDocsPerPartition = 500;
            Func<string, int, int, Stream> func = (id, p, f) => { return Utils.CreateRandomDocument(p, f, ref id); };
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, false);
            await container.ReplaceThroughputAsync(numPartitions * 10000);
            await container.ReplaceThroughputAsync(numPartitions * 100);
        }

        public async Task IngestDataForPartialSegmentsAsync(Container container, int numPartitions = 5, int fieldLength = 100, bool shouldInvalidate = false)
        {
            int numDocsPerPartition = 500;
            Func<string, int, int, Stream> func = (id, p, f) => { return Utils.CreateRandomDocument(p, f, ref id); };
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, shouldInvalidate);
        }

        public async Task IngestDataForInvalidationFilesAsync(Container container, int numPartitions = 3, int fieldLength = 500)
        {
            Console.WriteLine("Staring IngestDataForInvalidationFilesAsync...");
            int numDocsPerPartition = 500;
            Console.Write("\tIngesting data...");
            Func<string, int, int, Stream> func = (id, p, f) => { return Utils.CreateRandomDocument(p, f, ref id); };
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, true);
            Console.WriteLine("done.");
        }

        public async Task IngestDataForManyScenarios(Container container, int numPartitions = 5, int fieldLength = 100)
        {
            int numDocsPerPartition = 500;
            //await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(10000));
            //await Utils.WaitForOfferReplaceAsync(container);
            Func<string, int, int, Stream> func = (id, p, f) => { return Utils.CreateRandomDocument(p, f, ref id); };
            //await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, true);
            //await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput((numPartitions - 1) * 10000));
            //await Utils.WaitForOfferReplaceAsync(container);
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, fieldLength, true);
        }

        public async Task IngestDataForDataTypes(Container container, int numPartitions = 5)
        {
            int numDocsPerPartition = 1000;
            Func<string, int, int, Stream> func = (id, p, f) =>
            {
                DataTypeDocument doc = DataTypeDocument.CreateRandomDocument(id, p, f);
                return doc.SerializeToStream();
            };

            await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(10000));
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, 100, true);
        }

        public async Task IngestDataForSchemaEvolution(Container container, int numPartitions = 5)
        {
            int numDocsPerPartition = 500;
            Func<string, int, int, Stream> func = (id, p, f) =>
            {
                DataTypeDocument doc = DataTypeDocument.CreateRandomDocument(id, p, f);
                return doc.SerializeToStream();
            };
            await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(10000));
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, 100, true);

            func = (id, p, f) =>
            {
                SchemaEvolvedDocument doc = SchemaEvolvedDocument.CreateRandomDocument(id, p, f);
                return doc.SerializeToStream();
            };
            await Utils.UploadRandomDataAsync(container, func, numPartitions, numDocsPerPartition, 100, true);
        }
    }
}