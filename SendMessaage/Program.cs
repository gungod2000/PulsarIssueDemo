using PulsarFactory;


using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

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
                var messageId = PulsarHelper.SendMessage("1111");
                Console.WriteLine(messageId);
                Thread.Sleep(1000);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }

        }
    }
}