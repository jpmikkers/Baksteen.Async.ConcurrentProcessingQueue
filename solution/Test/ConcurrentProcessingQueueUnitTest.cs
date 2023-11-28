namespace Test;

using Baksteen.Async;
using System.Collections.Concurrent;

[TestClass]
public class ConcurrentProcessingQueueUnitTest
{
    [TestMethod]
    public async Task ZeroCapacityThrows()
    {
        await Assert.ThrowsExceptionAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using(var cpq = new ConcurrentProcessingQueue<int>(0, 1, async (item, ct) => { await Task.CompletedTask; }))
            {
            }
        });
    }

    [TestMethod]
    public async Task ZeroParallelismThrows()
    {
        await Assert.ThrowsExceptionAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using(var cpq = new ConcurrentProcessingQueue<int>(1, 0, async (item, ct) => { await Task.CompletedTask; }))
            {
            }
        });
    }

    [TestMethod]
    public async Task SingleThreadIsSequential()
    {
        ConcurrentQueue<int> resultQueue = new();

        await using(var cpq = new ConcurrentProcessingQueue<int>(5, 1, async (item, ct) =>
        {
            resultQueue.Enqueue(item);
            await Task.Delay(500 - item * 50, ct);
        }))
        {
            for(int t = 0; t < 10; t++)
            {
                await cpq.Enqueue(t);
            }
            await cpq.WaitForCompletion();
        }

        CollectionAssert.AreEqual(resultQueue, new[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    }

    [TestMethod]
    [Timeout(5000)]
    public async Task TestParallelExecution()
    {
        TaskCompletionSource taskCompletionSource = new ();
        int enterCount = 0;
        int exitCount = 0;

        await using(var cpq = new ConcurrentProcessingQueue<int>(5, 5, async (item, ct) =>
        {
            Interlocked.Increment(ref enterCount);
            await taskCompletionSource.Task;
            Interlocked.Increment(ref exitCount);
        }))
        {
            for(int t = 0; t < 10; t++)
            {
                await cpq.Enqueue(t);
            }

            await Task.Delay(100);              // give threads some time to enter
            Assert.AreEqual(5,enterCount);      // verify 5 threads entered execution
            Assert.AreEqual(0,exitCount);       // and none of them could have completed yet

            taskCompletionSource.SetResult();   // allow the threads to continue
            await cpq.WaitForCompletion();

            Assert.AreEqual(10, enterCount);
            Assert.AreEqual(10, exitCount);
        }
    }

    [TestMethod]
    [Timeout(2000)]
    public async Task FullQueueBlocks()
    {
        CancellationTokenSource cts = new();

        await using(var cpq = new ConcurrentProcessingQueue<int>(2, 2, async (item, ct) =>
        {
            await Task.Delay(1000000, ct);
        }, cts.Token))
        {
            // enqueueing will block so spawn a task for it
            var enqueueTask = Task.Run(async () =>
            {
                for(int t = 0; t < 5; t++)
                {
                    await cpq.Enqueue(t);
                }
            });

            await Task.Delay(500);
            Assert.IsFalse(enqueueTask.IsCompleted);    // should be blocked

            cts.Cancel();

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(async () => { await enqueueTask; });
        }
    }
}
