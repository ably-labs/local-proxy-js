import { CompletableDeferred } from './completable-deferred';

type AsyncTask = () => Promise<void>;

export class AsyncQueue {
  private readonly queue: AsyncTask[] = [];
  private processing: boolean = false;

  enqueue(task: AsyncTask): Promise<void> {
    const deferredValue = CompletableDeferred<void>();
    this.queue.push(async () => {
      try {
        await task();
      } finally {
        deferredValue.complete();
      }
    });
    this.processNext();
    return deferredValue.get()
  }

  private async processNext() {
    if (this.processing || this.queue.length === 0) return;

    this.processing = true;

    const task = this.queue.shift();

    try {
      await task!!();
    } finally {
      this.processing = false;
      this.processNext();
    }
  }
}
