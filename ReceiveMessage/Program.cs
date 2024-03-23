using PulsarFactory;

internal static class Program
{
    private static void Main()
    {
        Console.WriteLine("Init client pool...");
        PulsarHelper.InitPulsarClientPool();

        Console.WriteLine("Init producer pool...");
        PulsarHelper.InitProcedurePool();

        Console.WriteLine("Init consumer pool...");
        PulsarHelper.InitConsumerPool();


        while (true)
        {
            try
            {
                var (messageId, message) = PulsarHelper.ReceiveMessage();
                Console.WriteLine($"MessageId:{messageId} Message:{message}");
                Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                Thread.Sleep(1000);
                Console.WriteLine(ex.StackTrace);
            }

        }
    }
}