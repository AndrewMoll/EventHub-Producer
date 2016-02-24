using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Specialized;



/* Program Summary:  This Program is a data producer for testing your event hubs and Stream Analyitc queries.
 * Can produce around 2k events per second per thread through sending events directly to event hub partitions.
 * Automatically uses 90% of avaliable threads.  Can be changed in config file by specifiying a number
 * Set variables in the app.config file and look at the data object to understand format of the JSON object.  
 */


namespace DataProducer
{
    class Program
    {
       
        static int numThreads = (int)Math.Floor(.9*Environment.ProcessorCount);
        static string eventHubName = ConfigurationManager.AppSettings["eventHubName"];
        static string connectionString = ConfigurationManager.AppSettings["sbConnectionString"];


        static void Main(string[] args)
        {
            //Allows for number of threads to be specified 
            try
            {
                numThreads = Convert.ToInt32(ConfigurationManager.AppSettings["numThreads"]);
            }
            catch { }

            //Set up connections to Service Bus and create the client object
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString);
            builder.TransportType = TransportType.Amqp;
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(builder.ToString());
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(connectionString);
            EventHubDescription description = manager.CreateEventHubIfNotExists(eventHubName);
            var client = factory.CreateEventHubClient(eventHubName);
            
            //Creates a list of avaliable event hub senders based upon the number of partitions of the event hub
            //and avaliable threads
            int x = 0;
            List<EventHubSender> senderList = new List<EventHubSender>();
            while (x != description.PartitionCount)
            {
                EventHubSender partitionedSender = client.CreatePartitionedSender(description.PartitionIds[x]);
                senderList.Add(partitionedSender);
                x = x + 1;
            }
            var subLists = SplitToSublists(senderList);
            

            //create a list of tasks that independently send events to the event hub
            List<Task> taskList = new List<Task>();
            for (int i = 0; i < (int)numThreads; i++)
            {
                int indexOfSublist = i;
                taskList.Add(new Task(() => SingleTask.Run(subLists[indexOfSublist])));
            }

            if (numThreads == subLists.Count)
            {
                Console.WriteLine("Using " + numThreads + " threads.  Press enter to continue and produce data");
                Console.ReadKey();
            }
            else
            { 
                Console.WriteLine("Number of threads != number of sender arrays.  Tasks will not start.");
                Console.Read();
            }

            //Start Each Event
            taskList.ForEach(a => a.Start());

            //Wait for all to end.  This shouldn't happen but need this here or the project would close
            taskList.ForEach(a => a.Wait());


        }

        private static List<List<EventHubSender>> SplitToSublists(List<EventHubSender> source)
        {
            return source
                     .Select((x, i) => new { Index = i, Value = x })
                     .GroupBy(x => x.Index % numThreads)
                     .Select(x => x.Select(v => v.Value).ToList())
                     .ToList();
        }



    }     
}
