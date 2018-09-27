using Caf.Etl.Models.CosmosDBSqlApi.EtlEvent;
using Caf.Etl.Models.CosmosDBSqlApi.Sample;
using Caf.Etl.Models.Manual.TidyData;
using Caf.Etl.Models.Manual.TidyData.DataTables;
using Caf.Etl.Nodes.CosmosDBSqlApi.Load;
using Caf.Etl.Nodes.Manual.Extract;
using Caf.Etl.Nodes.Manual.Mappers;
using Caf.Etl.Nodes.Manual.Transform;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace SoilGridPointToCosmosDB
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().Wait();
        }

        static async Task MainAsync()
        {
            EtlEvent etlEvent = new EtlEvent(
                "EtlEvent",
                "LocalProcess",
                "http://files.cafltar.org/data/schema/documentDb/v2/etlEvent.json",
                "CookEastSoilGridPointSurvey",
                "1.0",
                "CookEastSoilGridPointSurvey_DotNet_SoilGridPointToCosmosDB",
                DateTime.UtcNow);

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();

            JsonSerializerSettings serializerSettings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };

            string data = configuration["PathToData"];
            string dict = configuration["PathToDictionary"];

            if (!File.Exists(data) |
               !File.Exists(dict))
                throw new FileNotFoundException();

            etlEvent.Inputs.Add(data);
            etlEvent.Inputs.Add(dict);

            DocumentClient client;
            try
            {
                client = new DocumentClient(
                    new Uri(
                        configuration["CosmosServiceEndpoint"]),
                        configuration["CosmosAuthKey"],
                        serializerSettings);
            }
            catch(Exception e)
            {
                etlEvent.Logs.Add(
                    $"Error creating DocumentClient: {e.Message}");
                throw new Exception("Error creating DocumentClient", e);
            }  

            var extractor = new TidyDataCsvExtractor(
                configuration["PathToData"],
                configuration["PathToDictionary"]);
            
            var transformer = new CosmosDBSqlApiSampleV2Transformer
                    <SoilGridPointSurveyV1, SoilSample>(
                new MapFromSoilGridPointSurveyToSoilSample(),
                "http://files.cafltar.org/data/schema/documentDb/v2/sample.json",
                etlEvent.Id,
                "CookEastSoilGridPointSurvey",
                "CookEast",
                "SoilSample");

            var loader = new DocumentLoader(
                client,
                "cafdb",
                "items");

            try
            {
                TidyData extracted = extractor.Extract<SoilGridPointSurveyV1>();

                etlEvent.Logs.Add(
                    $"Extracted TidyData with {extracted.Observations.Count} observations");

                List<SoilSample> transformed = transformer.Transform(extracted);

                etlEvent.Logs.Add(
                    $"Transformed TidyData to {transformed.Count} SoilSamples");

                //StoredProcedureResponse<bool>[] results = await loader.LoadBulk(transformed);

                int docsLoaded = 0;

                foreach(SoilSample sample in transformed)
                {
                    ResourceResponse<Document> result = 
                        await loader.LoadNoReplace(sample);
                    if(result.StatusCode == System.Net.HttpStatusCode.Created)
                    {
                        etlEvent.Outputs.Add(result.Resource.Id);
                        docsLoaded++;
                    }

                    Console.Write(".");
                }

                etlEvent.Logs.Add(
                    $"Loaded {docsLoaded.ToString()} SoilSamples");
            }
            catch(Exception e)
            {
                etlEvent.Logs.Add(
                    $"Error in ETL pipeline: {e.Message}");
                throw new Exception("Error in ETL pipeline", e);
            }
            finally
            {
                etlEvent.DateTimeEnd = DateTime.UtcNow;
                ResourceResponse<Document> result = await loader.LoadNoReplace(etlEvent);
            }
            
        }
    }
}
