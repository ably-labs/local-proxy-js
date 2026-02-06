type CompletionHandler<T = void> = (value: T) => void

class DefaultCompletableDeferred<T = void> {
  private _completeWith!: CompletionHandler<T>;
  private value: T | undefined

  private valuePromise = new Promise<T>(resolve => {
    this._completeWith = resolve;
  });

  public complete(value: T): void {
    this._completeWith(value);
  }

  public async get(): Promise<T> {
    return this.value ?? this.valuePromise;
  }
}

export function CompletableDeferred<T>() {
  return new DefaultCompletableDeferred<T>()
}
