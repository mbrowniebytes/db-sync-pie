import concurrent.futures
import itertools
import time


class SyncThread:
    def __init__(self, config, log):
        self.config = config
        self.log = log

        self.max_threads = self.config.max_threads
        if self.max_threads <= 0:
            raise ValueError(f"SyncThread: Max threads {self.max_threads} must be 1 or greater")

        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_threads, thread_name_prefix="db-sync-pie"
        )

        self.task_counter = itertools.count(1)
        self.tables = []
        self.submit_function = None
        self.callback_function = None
        self.futures = {}

    def _pool_submit(self):
        if len(self.tables) == 0:
            return
        task_id = next(self.task_counter)
        table = self.tables.pop(0)
        table["task_id"] = task_id
        self.log.info(f"Process {table} task id {task_id}")
        future = self.executor.submit(self.submit_function, table)
        future.add_done_callback(self.callback_function)
        self.futures[future] = task_id

    def pool(self, tables, submit_function, callback_function):
        self.tables = tables.copy()
        self.submit_function = submit_function
        self.callback_function = callback_function

        nbr_tables = len(tables)
        if nbr_tables == 0:
            raise ValueError("No tables provided")

        results = []
        start = time.perf_counter()
        try:
            self.log.info(f"Processing {nbr_tables} tables using ThreadPoolExecutor in {self.max_threads} threads")

            with self.executor:
                # Submit initial batch of tasks
                for _ in range(self.max_threads):
                    self._pool_submit()

                # Process tasks dynamically
                while self.futures:
                    done, not_done = concurrent.futures.wait(
                        self.futures, return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    for future in done:
                        # remove completed task
                        task_id = self.futures.pop(future)

                        result = future.result()
                        # self.log.info("thread pool future result", result=result)
                        results.append(result)

                        if task_id < nbr_tables:
                            # submit a new task if we haven't reached the total task limit
                            self._pool_submit()
                        # else: self.log.info("thread pool futures Finishing")

                self.executor.shutdown(wait=True, cancel_futures=False)
        except Exception as e:
            self.log.error(f"Task {task_id} failed", e=e)
            raise Exception(e)

        # self._sync_insert([{"table":"Dealerships_20250212", "primary": "id", "modified": "timestamp"}])

        finish = time.perf_counter()
        elapsed = round(finish - start, 2)
        self.log.info(f"It took {elapsed}s to finish.")
        return results


def main():
    print("not directly callable")


if __name__ == "__main__":
    main()
