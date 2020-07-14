using CosmosDB;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;

namespace TestDataCreator
{
    public class SchemaEvolvedDocument
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        public int PartitionKey { get; set; }

        public int BooleanField { get; set; } = 0;

        public List<string> ArrayField { get; set; } = new List<string>();

        public string NumberField { get; set; } = "0.0";

        public string StringField { get; set; } = string.Empty;

        public SchemaEvolvedDocument ObjectField { get; set; } = null;

        public Stream SerializeToStream()
        {
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            string json = JsonConvert.SerializeObject(this);
            writer.Write(json);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        public static SchemaEvolvedDocument CreateRandomDocument(string id, int partition, int fieldLength, int level = 0)
        {
            SchemaEvolvedDocument retval = new SchemaEvolvedDocument();
            retval.Id = id;
            retval.PartitionKey = partition;
            retval.BooleanField = Utils.CreateRandomInt();
            retval.NumberField = Utils.CreateRandomString(fieldLength);
            retval.StringField = Utils.CreateRandomString(fieldLength);

            if (level >= 3)
            {
                retval.ObjectField = null;
            }
            else
            {
                SchemaEvolvedDocument objectField = Utils.CreateRandomBool() ? CreateRandomDocument(id, partition, fieldLength, level + 1) : null;
                retval.ObjectField = objectField;
            }

            List<string> arrayField = new List<string>() { Utils.CreateRandomString(), Utils.CreateRandomString(), Utils.CreateRandomString() };
            retval.ArrayField = arrayField;

            return retval;
        }
    }
}
