using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace CosmosDB
{
    public static class Utils
    {
        public static string CreateRandomString(int numChars = 25)
        {
            const string allChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            Random rand = new Random();
            StringBuilder retval = new StringBuilder();
            for (int i = 0; i < numChars; ++i)
            {
                retval.Append(allChars[rand.Next(0, allChars.Length - 1)]);
            }
            return retval.ToString();
        }

        public static bool CreateRandomBool()
        {
            Random rand = new Random();
            int randInt = rand.Next(10) % 2;
            return (randInt == 0) ? false : true;
        }

        public static double CreateRandomDouble()
        {
            Random rand = new Random();
            return rand.NextDouble();
        }

        public static int CreateRandomInt()
        {
            Random rand = new Random();
            return rand.Next();
        }

        public static T PickRandomItem<T>(List<T> items)
        {
            Random rand = new Random();
            int index = rand.Next(0, items.Count - 1);
            return items[index];
        }

        public static Stream CreateRandomDocument(int partition, int fieldLength, ref string id)
        {
            JObject json = new JObject();
            id = string.IsNullOrWhiteSpace(id)? Guid.NewGuid().ToString() : id;
            json.Add("id", id);
            json.Add("PartitionKey", partition);
            for (int i = 0; i < 100; ++i)
            {
                json.Add($"Field{i}", CreateRandomString(fieldLength));
            }
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(json.ToString());
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        public static string GetMemberName<T>(Expression<Func<T>> lambda)
        {
            var me = lambda.Body as MemberExpression;
            if (me == null)
            {
                throw new ArgumentException("You must pass a lambda of the form: '() => Class.Property' or '() => object.Property'");
            }
            return me.Member.Name;
        }

        public static Task UploadRandomDataAsync(Container container, Func<string, int, int, Stream> createDocFunc,
            int numPartitions, int numDocsPerPartition, int fieldLength, bool shouldInvalidate = false)
        {
            List<Task> tasks = new List<Task>();
            for (int document = 0; document < numDocsPerPartition; ++document)
            {
                for (int partition = 0; partition < numPartitions; ++partition)
                {
                    string id = Guid.NewGuid().ToString();
                    int p = partition;
                    using (Stream stream = createDocFunc(id, partition, fieldLength))
                    {
                        tasks.Add(container.CreateItemStreamAsync(stream, new PartitionKey(p)).ContinueWith((itemResponse) =>
                        {
                            if (!itemResponse.Result.IsSuccessStatusCode)
                            {
                                throw new Exception("Error when uploading stream.");
                            }

                            if (shouldInvalidate && ShouldInvalidate())
                            {
                                using (Stream invStream = createDocFunc(id, p, fieldLength))
                                {
                                    {
                                        try
                                        {
                                            container.UpsertItemStreamAsync(invStream, new PartitionKey(p)).Wait();
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Exception caught: {ex.Message}");
                                            partition--;
                                        }
                                    }
                                }
                            }
                        }));
                    }
                }
            }
            return Task.WhenAll(tasks);
        }

        public static async Task WaitForOfferReplaceAsync(Container container)
        {
            bool done = false;
            while (!done)
            {
                ThroughputResponse response = await container.ReadThroughputAsync(new RequestOptions());
                done = response.IsReplacePending == null || !response.IsReplacePending.Value;
                Thread.Sleep(60000);
            }
        }

        private static bool ShouldInvalidate()
        {
            Random rand = new Random();
            return rand.NextDouble() <= 0.4;
        }
    }
}
