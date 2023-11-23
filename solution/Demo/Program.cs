namespace Demo;

using Baksteen.Async;
using System;
using System.Threading;
using System.Threading.Tasks;

internal class Program
{
    static async Task Main(string[] args)
    {
        static async ValueTask processItem(int x, CancellationToken ct)
        {
            Console.WriteLine($"start processing item {x}");
            await Task.Delay(Random.Shared.Next(500,2000), ct).ConfigureAwait(false);
            Console.WriteLine($"done processing item {x}");
        }

        await using(var cpq = new ConcurrentProcessingQueue<int>(5, 2, processItem))
        {
            for(int t = 0; t < 10; t++)
            {
                Console.WriteLine($"enqueueing {t}");
                await cpq.Enqueue(t);
            }
            await cpq.WaitForCompletion();
        }
    }
}
