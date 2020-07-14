using Microsoft.Azure.Cosmos;
using System;
using System.Threading.Tasks;

/* *
 * Scenarios:
 * - partial file segments
 *  - ideally with fully flushed segments as well
 * - invalidations w/ merged invalidations as well
 * - splits with archived segments
 * - splits without archived segments
 * */
namespace CosmosDB
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string connStr = "";

            TestDataCreator dataCreator = new TestDataCreator(connStr);
            string keyName = "PartitionKey";
            Container container = await dataCreator.CreateContainerAsync("test-data", "schema-evol", $"/{keyName}");
            Console.WriteLine("Starting container...");
            //await dataCreator.IngestDataNoSplit(container);
            //await dataCreator.IngestDataWithSplit(container, 3);
            //await dataCreator.IngestDataForManyScenarios(container);
            //await dataCreator.IngestDataForDataTypes(container);
            await dataCreator.IngestDataForSchemaEvolution(container);
            Console.WriteLine("done.");



            //TestDataCreator dataCreator = new TestDataCreator(connStr);
            //string keyName = Utils.GetMemberName(() => new DummyDocument().PartitionKey);
            //Container container = await dataCreator.CreateContainerAsync("test-db", "test-coll-presplit", $"/{keyName}");
            //Console.Write("Starting pre-split container...");
            //await dataCreator.IngestDataForPreSplit(container);
            //Console.WriteLine("done.");
            //Console.Write("Starting post-split container...");
            //container = await dataCreator.CreateContainerAsync("test-db", "test-coll-postsplit", $"/{keyName}");
            //await dataCreator.IngestDataForPostSplit(container, 5, 500);
            //Console.WriteLine("done.");
        }
    }
}
