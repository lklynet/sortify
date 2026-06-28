export function createRateLimiter(minIntervalMs: number) {
  let lastCall = 0;
  return async <T>(fn: () => Promise<T>): Promise<T> => {
    const now = Date.now();
    const wait = Math.max(0, minIntervalMs - (now - lastCall));
    lastCall = now + wait + 1;
    if (wait > 0) {
      await new Promise((resolve) => setTimeout(resolve, wait));
    }
    return fn();
  };
}
