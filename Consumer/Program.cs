// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using Confluent.Kafka;
using Consumer;
using Microsoft.Extensions.Configuration;

Console.WriteLine("Kafka consumer client started!");

if (args.Length != 1)
{
    Console.WriteLine("Please provide the configuration file path as a command line argument");
}

// Source for config file
IConfiguration configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
          .AddIniFile(args[0])
          .Build();


configuration["group.id"] = "kafka-dotnet-getting-started";
configuration["auto.offset.reset"] = "earliest";

// reference the topic to source
const string topic = "Purchases";

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};


using (var consumer = new ConsumerBuilder<string, string>(configuration.AsEnumerable()).Build())
{
    // Subscribe to selected topic 
    consumer.Subscribe(topic);
    try
    {
        while (true)
        {
            var cr = consumer.Consume(cts.Token);

            User user = JsonSerializer.Deserialize<User>(cr.Message.Value);

            SendMailAsync(user, cts.Token);

            Console.WriteLine($"Consumed event from topic {topic} with Message: {cr.Message.Value}");
        }
    }
    catch (OperationCanceledException)
    {
        // Ctrl-C was pressed.
    }
    finally
    {
        consumer.Close();
    }
}

static async Task SendMailAsync(User user, CancellationToken txn)
{
    if (null == user)
    {
        Console.WriteLine("No event rreceived!");
        return;
    }

    var message = $"Dear {user.Name}, Your purchase of {user.Items.Count} was received! Your cart contains the following items {user.Items.Select(x => x + ", ")} will be delivered shortly";

    await Messenger.SendEmailAsync(user.Email, "Purchase received", message, txn);
}