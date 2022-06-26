import Lock from "../index"
import AsyncLockImpl, * as AsyncLockImplType from "async-lock"

export default class AsyncLock implements Lock {
  private readonly lock: AsyncLockImplType;

  constructor(lock: AsyncLockImplType = new AsyncLockImpl()) {
    this.lock = lock;
  }

  acquire<T>(key: string, withLockCallback: () => Promise<T>): Promise<T> {
    return this.lock.acquire(key, withLockCallback);
  }
}
