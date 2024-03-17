import sys
from multiprocessing import Process

from goit_cs_hw04_task01_main import prepare_tasks, finalize, Queue_files, jQueue_results, search_file


def worker_func(*args, **kwargs):
    search_file(*args, **kwargs)
    sys.exit(0)


def runner_main(argl, qf, jq):
    process_list = [Process(target=worker_func, args=(argl.key, qf, jq)) for _ in
                    range(min(argl.parallelism, qf.qsize()))]
    for t in process_list:
        t.start()

    for t in process_list:
        t.join()


if __name__ == '__main__':
    argl = prepare_tasks(Queue_files)
    runner_main(argl, Queue_files, jQueue_results)

    finalize(argl, jQueue_results)

