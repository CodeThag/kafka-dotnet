// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Producer;

Console.WriteLine("Kafka producer client started!");

if (args.Length != 1)
{
    Console.WriteLine("Please provide the configuration file path as a command line argument");
}

// Source for config file
IConfiguration configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddIniFile(args[0])
    .Build();

// Topics are case sensitive
const string topic = "Purchases";

// User and items to be purchased

string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

Random rand = new Random();

User[] users = {
    new User { Name = "eabara", Email = "eabara@mail.com", Items =  items.Take(rand.Next(1, items.Length)).ToList() },
    new User { Name = "jsmith", Email = "jsmith@mail.com" , Items =  items.Take(rand.Next(1, items.Length)).ToList() },
    new User { Name = "sgarcia", Email = "sgarcia@mail.com", Items =  items.Take(rand.Next(1, items.Length)).ToList() },
    new User { Name = "jbernard", Email = "jbernard@mail.com", Items =  items.Take(rand.Next(1, items.Length)).ToList() },
    new User { Name = "htanaka", Email = "htanaka@mail.com", Items =  items.Take(rand.Next(1, items.Length)).ToList() },
    new User { Name = "awalther", Email = "awalter@mail.com", Items =  items.Take(rand.Next(1, items.Length)).ToList() }
};


using (var producer = new ProducerBuilder<Null, string>(configuration.AsEnumerable()).Build())
{
    var numProduced = 0;
    const int numMessages = 10;

    for (int i = 0; i < numMessages; ++i)
    {

        // Randomly create purchase events
        Random rnd = new Random();
        var user = users[rnd.Next(users.Length)];

        var message = JsonSerializer.Serialize(user);

        // new Message<string, string> store events a key-value pair for messaging
        producer.Produce(topic, new Message<Null, string> { Value = message }, (deliveryReport) =>
        {
            if (deliveryReport.Error.Code != ErrorCode.NoError)
            {
                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
            }
            else
            {
                Console.WriteLine($"Produced event to topic {topic}: Message = { message }");
                numProduced += 1;
            }
        });
    }

    producer.Flush(TimeSpan.FromSeconds(10));
    Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
}
