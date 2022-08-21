using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace TigerBeetle.Benchmarks
{
	internal class TimedQueue
	{
		#region Fields

		private Stopwatch timer = new Stopwatch();

		#endregion Fields

		#region Properties

		public Queue<Delegate> Batches { get; } = new();

		public long MaxTransfersLatency { get; private set; }

		public long TotalTime { get; private set; }

		#endregion Properties

		#region Methods

		public async Task ExecuteAsync()
		{
			while (Batches.TryPeek(out Delegate func))
			{
				Func<Task> action = func as Func<Task>;
				timer.Restart();
				await action();
				timer.Stop();

				TotalTime += timer.ElapsedMilliseconds;

				_ = Batches.Dequeue();
				
				MaxTransfersLatency = Math.Max(timer.ElapsedMilliseconds, MaxTransfersLatency);
			}
		}

		public void Execute()
		{
			while (Batches.TryPeek(out Delegate func))
			{
				Action action = func as Action;

				timer.Restart();
				action();
				timer.Stop();

				TotalTime += timer.ElapsedMilliseconds;

				_ = Batches.Dequeue();

				MaxTransfersLatency = Math.Max(timer.ElapsedMilliseconds, MaxTransfersLatency);
			}
		}

		public void Reset()
		{
			TotalTime = 0;
			MaxTransfersLatency = 0;
			timer.Reset();
			Batches.Clear();
		}

		#endregion Methods
	}

}
