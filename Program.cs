using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;

namespace FhirLoader
{
    class Program
    {
        private static readonly HttpClient httpClient = new HttpClient();
        static void Main(string inputFolder, Uri fhirServerUrl, Uri authority = null, string clientId = null, string clientSecret = null, string bufferFileName = "resources.json", bool reCreateBufferIfExists = false, int maxDegreeOfParallelism = 64)
        {

            if (!(new FileInfo(bufferFileName).Exists) || reCreateBufferIfExists)
            {
                CreateBufferFile(inputFolder, bufferFileName);
            }

            AuthenticationContext authContext = new AuthenticationContext(authority.AbsoluteUri, new TokenCache());;
            ClientCredential clientCredential = new ClientCredential(clientId, clientSecret);

            var randomGenerator = new Random();

            var actionBlock = new ActionBlock<string>(async resourceString =>
            {
                var resource = JObject.Parse(resourceString);
                string resource_type = (string)resource["resourceType"];
                string id = (string)resource["id"];

                Thread.Sleep(TimeSpan.FromMilliseconds(randomGenerator.Next(50)));

                StringContent content = new StringContent(resourceString, Encoding.UTF8, "application/json");
                var pollyDelays =
                        new[]
                        {
                                TimeSpan.FromMilliseconds(2000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(3000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(5000 + randomGenerator.Next(50)),
                                TimeSpan.FromMilliseconds(8000 + randomGenerator.Next(50))
                        };

                var authResult = authContext.AcquireTokenAsync(fhirServerUrl.AbsoluteUri.TrimEnd('/'), clientCredential).Result;
 
                HttpResponseMessage uploadResult = await Policy
                    .HandleResult<HttpResponseMessage>(response => !response.IsSuccessStatusCode)
                    .WaitAndRetryAsync(pollyDelays, (result, timeSpan, retryCount, context) =>
                    {
                        Console.WriteLine($"Request failed with {result.Result.StatusCode}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}");
                    })
                    .ExecuteAsync(() =>
                    {
                        var message = string.IsNullOrEmpty(id)
                            ? new HttpRequestMessage(HttpMethod.Post, new Uri(fhirServerUrl, $"/{resource_type}"))
                            : new HttpRequestMessage(HttpMethod.Put, new Uri(fhirServerUrl, $"/{resource_type}/{id}"));

                        message.Content = content;
                        message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);
                        return httpClient.SendAsync(message);
                    });

                if (!uploadResult.IsSuccessStatusCode)
                {
                    string resultContent = await uploadResult.Content.ReadAsStringAsync();
                    Console.WriteLine(resultContent);
                    throw new Exception($"Unable to upload to server. Error code {uploadResult.StatusCode}");
                }
            },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = maxDegreeOfParallelism
                }
            );

            System.IO.StreamReader buffer = new System.IO.StreamReader(bufferFileName);
            string line;
            while ((line = buffer.ReadLine()) != null)
            {
                actionBlock.Post(line);
            }

            actionBlock.Complete();
            actionBlock.Completion.Wait();
        }

        private static void CreateBufferFile(string inputFolder, string bufferFileName)
        {
            using (System.IO.StreamWriter outFile = new System.IO.StreamWriter(bufferFileName))
            {
                string[] files = Directory.GetFiles(inputFolder, "*.json", SearchOption.TopDirectoryOnly);

                foreach (var file in files)
                {
                    string bundleText = File.ReadAllText(file);

                    JObject bundle;
                    try
                    {
                        bundle = JObject.Parse(bundleText);
                    }
                    catch (JsonReaderException)
                    {
                        Console.WriteLine("Input file is not a valid JSON document");
                        throw;
                    }

                    try
                    {
                        SyntheaReferenceResolver.ConvertUUIDs(bundle);
                    }
                    catch
                    {
                        Console.WriteLine("Failed to resolve references in doc");
                        throw;
                    }

                    foreach (var r in bundle.SelectTokens("$.entry[*].resource"))
                    {
                        outFile.WriteLine(r.ToString(Formatting.None));
                    }
                }
            }
        }
    }
}
