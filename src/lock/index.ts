type Lock = {
  acquire: <T>(key: string, withLockCallback: () => Promise<T>) => Promise<T>;
};

export default Lock;
