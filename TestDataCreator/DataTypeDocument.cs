using CosmosDB;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

namespace TestDataCreator
{
    public class DataTypeDocument
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        public int PartitionKey { get; set; }

        public bool BooleanField { get; set; } = false;

        public List<string> ArrayField { get; set; } = new List<string>();

        public double NumberField { get; set; } = 0.0;

        public string StringField { get; set; } = string.Empty;

        public DataTypeDocument ObjectField { get; set; } = null;

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

        public static DataTypeDocument CreateRandomDocument(string id, int partition, int fieldLength, int level = 0)
        {
            DataTypeDocument retval = new DataTypeDocument();
            retval.Id = id;
            retval.PartitionKey = partition;
            retval.BooleanField = Utils.CreateRandomBool();
            retval.NumberField = Utils.CreateRandomDouble();
            retval.StringField = Utils.CreateRandomString(fieldLength);

            if (level >= 3)
            {
                retval.ObjectField = null;
            }
            else
            {
                DataTypeDocument objectField = Utils.CreateRandomBool() ? CreateRandomDocument(id, partition, fieldLength, level + 1) : null;
                retval.ObjectField = objectField;
            }

            List<string> arrayField = new List<string>() { Utils.CreateRandomString(), Utils.CreateRandomString(), Utils.CreateRandomString() };
            retval.ArrayField = arrayField;

            return retval;
        }
    }
}
