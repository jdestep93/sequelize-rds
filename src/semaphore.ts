export class Semaphore {
  #available: number;
  #waiters: Array<() => void> = [];

  constructor(readonly max: number) {
    this.#available = max;
  }

  async acquire(): Promise<void> {
    if (this.#available > 0) {
      this.#available -= 1;
      return;
    }

    await new Promise<void>((resolve) => {
      this.#waiters.push(resolve);
    });
  }

  release(): void {
    const waiter = this.#waiters.shift();

    if (waiter) {
      waiter();
      return;
    }

    this.#available = Math.min(this.#available + 1, this.max);
  }

  get waiting(): number {
    return this.#waiters.length;
  }
}
