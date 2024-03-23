using DotPulsar;
using DotPulsar.Extensions;
using System.Diagnostics;
using System.Threading;
using PulsarFactory;
using Microsoft.VisualBasic.ApplicationServices;
using static DotPulsar.Internal.PulsarApi.CommandGetTopicsOfNamespace;
using System;

namespace PulsarIssueDemo
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private static bool isStopSend = false;

        private static bool isStopReceive = false;

        private void Form1_Load(object sender, EventArgs e)
        {
            //init 
            PulsarHelper.InitPulsarClientPool();

            PulsarHelper.InitProcedurePool();

            PulsarHelper.InitConsumerPool();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            isStopSend = false;
            Task.Factory.StartNew(() =>
            {
                while (!isStopSend)
                {
                    try
                    {
                        var messageId = PulsarHelper.SendMessage("1111");
                        Trace.WriteLine(messageId);
                        Thread.Sleep(1000);
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine(ex.StackTrace);
                    }

                }

                Trace.WriteLine("stop send message..");
            });
        }

        private void button2_Click(object sender, EventArgs e)
        {
            isStopSend = true;
        }

        private void button3_Click(object sender, EventArgs e)
        {
            //引发的异常:“System.Threading.Tasks.TaskCanceledException”(位于 System.Private.CoreLib.dll 中)
            //     MessageId: 231:3135:-1:-1:persistent://public/default/mytopic Message:1111
            //     MessageId: 231:3138:-1:-1:persistent://public/default/mytopic Message:1111
            //     MessageId: 231:3252:-1:-1:persistent://public/default/mytopic Message:1111
            //         at DotPulsar.Internal.ConsumerChannel`1.Receive(CancellationToken cancellationToken)
            //at DotPulsar.Internal.SubConsumer`1.InternalReceive(CancellationToken cancellationToken)
            //at DotPulsar.Internal.Executor.TryExecuteOnce[TResult](Func`1 func, CancellationToken cancellationToken)
            //at DotPulsar.Internal.Executor.TryExecuteOnce[TResult](Func`1 func, CancellationToken cancellationToken)
            //at DotPulsar.Internal.Executor.Execute[TResult](Func`1 func, CancellationToken cancellationToken)
            //at DotPulsar.Internal.SubConsumer`1.Receive(CancellationToken cancellationToken)
            //at DotPulsar.Internal.Consumer`1.Receive(CancellationToken cancellationToken)
            //at PulsarFactory.PulsarHelper.ReceiveMessage() in C: \Users\golde\source\repos\PulsarIssueDemo\PulsarFactory\PulsarHelper.cs:line 109
            //at PulsarIssueDemo.Form1.<> c.< button3_Click > b__6_0() in C: \Users\golde\source\repos\PulsarIssueDemo\PulsarIssueDemo\Form1.cs:line 101

            isStopReceive = false;
            for (int i = 0; i < 10; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (!isStopReceive)
                    {
                        try
                        {
                            var (messageId, message) = PulsarHelper.ReceiveMessage();
                            Trace.WriteLine($"MessageId:{messageId} Message:{message}");
                            Thread.Sleep(500);
                        }
                        catch (Exception ex)
                        {
                            Thread.Sleep(1000);
                            Trace.WriteLine(ex.StackTrace);
                        }

                    }

                    Trace.WriteLine("stop send message..");
                });
            }
        }

        private void button4_Click(object sender, EventArgs e)
        {
            //引发的异常:“System.Threading.Tasks.TaskCanceledException”(位于 System.Private.CoreLib.dll 中)
            //     MessageId: 231:3135:-1:-1:persistent://public/default/mytopic Message:1111
            //     MessageId: 231:3138:-1:-1:persistent://public/default/mytopic Message:1111
            //     MessageId: 231:3252:-1:-1:persistent://public/default/mytopic Message:1111
            //         at DotPulsar.Internal.ConsumerChannel`1.Receive(CancellationToken cancellationToken)
            //at DotPulsar.Internal.SubConsumer`1.InternalReceive(CancellationToken cancellationToken)
            //at DotPulsar.Internal.Executor.TryExecuteOnce[TResult](Func`1 func, CancellationToken cancellationToken)
            //at DotPulsar.Internal.Executor.TryExecuteOnce[TResult](Func`1 func, CancellationToken cancellationToken)
            //at DotPulsar.Internal.Executor.Execute[TResult](Func`1 func, CancellationToken cancellationToken)
            //at DotPulsar.Internal.SubConsumer`1.Receive(CancellationToken cancellationToken)
            //at DotPulsar.Internal.Consumer`1.Receive(CancellationToken cancellationToken)
            //at PulsarFactory.PulsarHelper.ReceiveMessage() in C: \Users\golde\source\repos\PulsarIssueDemo\PulsarFactory\PulsarHelper.cs:line 109
            //at PulsarIssueDemo.Form1.<> c.< button3_Click > b__6_0() in C: \Users\golde\source\repos\PulsarIssueDemo\PulsarIssueDemo\Form1.cs:line 101
            isStopReceive = false;
            for (int i = 0; i < 10; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (!isStopReceive)
                    {
                        try
                        {
                            var (messageId, message) = PulsarHelper.ReceiveMessage1();
                            Trace.WriteLine($"MessageId:{messageId} Message:{message}");
                            Thread.Sleep(500);
                        }
                        catch (Exception ex)
                        {
                            Thread.Sleep(1000);
                            Trace.WriteLine(ex.StackTrace);
                        }

                    }

                    Trace.WriteLine("stop send message..");
                });
            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            //引发的异常:“System.OperationCanceledException”(位于 System.Private.CoreLib.dll 中)
            //The consumer for topic 'persistent://public/default/mytopic' changed state to 'Active'
            //The consumer for topic 'persistent://public/default/mytopic' changed state to 'Active'
            //   at System.Threading.CancellationToken.ThrowOperationCanceledException()
            //   at System.Threading.SemaphoreSlim.WaitUntilCountOrTimeoutAsync(TaskNode asyncWaiter, Int32 millisecondsTimeout, CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Guard(CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Receive(CancellationToken cancellationToken)
            //   at PulsarFactory.PulsarHelper.ReceiveMessage1() in C:\Users\golde\source\repos\PulsarIssueDemo\PulsarFactory\PulsarHelper.cs:line 156
            //   at PulsarIssueDemo.Form1.re() in C:\Users\golde\source\repos\PulsarIssueDemo\PulsarIssueDemo\Form1.cs:line 173
            //   at System.Threading.CancellationToken.ThrowOperationCanceledException()
            //   at System.Threading.SemaphoreSlim.WaitUntilCountOrTimeoutAsync(TaskNode asyncWaiter, Int32 millisecondsTimeout, CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Guard(CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Receive(CancellationToken cancellationToken)
            //   at PulsarFactory.PulsarHelper.ReceiveMessage1() in C:\Users\golde\source\repos\PulsarIssueDemo\PulsarFactory\PulsarHelper.cs:line 156
            //   at PulsarIssueDemo.Form1.re() in C:\Users\golde\source\repos\PulsarIssueDemo\PulsarIssueDemo\Form1.cs:line 173
            //   at System.Threading.CancellationToken.ThrowOperationCanceledException()
            //   at System.Threading.SemaphoreSlim.WaitUntilCountOrTimeoutAsync(TaskNode asyncWaiter, Int32 millisecondsTimeout, CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Guard(CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Receive(CancellationToken cancellationToken)
            //   at PulsarFactory.PulsarHelper.ReceiveMessage1() in C:\Users\golde\source\repos\PulsarIssueDemo\PulsarFactory\PulsarHelper.cs:line 156
            //   at PulsarIssueDemo.Form1.re() in C:\Users\golde\source\repos\PulsarIssueDemo\PulsarIssueDemo\Form1.cs:line 173
            //   at System.Threading.CancellationToken.ThrowOperationCanceledException()
            //   at System.Threading.SemaphoreSlim.WaitUntilCountOrTimeoutAsync(TaskNode asyncWaiter, Int32 millisecondsTimeout, CancellationToken cancellationToken)
            //   at DotPulsar.Internal.Consumer`1.Guard(CancellationToken cancellationToken)

            isStopReceive = false;
            for (int i = 0; i < 10; i++)
            {
                Task.Factory.StartNew(() =>
                {

                    for (int i = 0; i < 10; i++)
                    {
                        Task.Factory.StartNew(() =>
                        {

                            re();

                        });
                    }

                });
            }
        }

        private void re()
        {
            while (!isStopReceive)
            {
                try
                {
                    var (messageId, message) = PulsarHelper.ReceiveMessage1();
                    Trace.WriteLine($"MessageId:{messageId} Message:{message}");
                    Thread.Sleep(500);
                }
                catch (Exception ex)
                {
                    Thread.Sleep(1000);
                    Trace.WriteLine(ex.StackTrace);
                }

            }
        }
    }
}
