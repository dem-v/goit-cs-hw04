import argparse
import glob
import logging
import logging.config
import logging.handlers
from multiprocessing import Queue, JoinableQueue
from pathlib import Path
from threading import Thread
from time import sleep
from timeit import timeit

MAX_THREADS = 10
Queue_files = Queue()
jQueue_results = JoinableQueue()

format = '%(asctime)s %(process)d %(threadName)s %(message)s'
logging.basicConfig(format=format, level=logging.DEBUG, datefmt='%H:%M:%S')


def time_wrapper(func):
    def wrapper(*args, **kwargs):
        time = timeit(lambda: func(*args, **kwargs), number=1, globals=globals())
        logging.warning(f'Total time: {time:.6f} sec')

    return wrapper


@time_wrapper
def search_file(key: list[str], in_q: Queue, out_jq: JoinableQueue):
    logging.debug(f'Start searching for {key}')

    while not in_q.empty():
        try:
            file = in_q.get(False, timeout=1)
            try:
                logging.debug(f'Reading {file}')
                with open(file, 'r') as f:
                    for k in key:
                        if k in f.read():
                            out_jq.put(file)
                            logging.info(f'Found {k} in {file}')
                            out_jq.task_done()
            except FileNotFoundError:
                logging.error(f'File {file} not found')
            except PermissionError:
                logging.warning(f'Permission denied for {file}')
            except Exception as e:
                logging.critical(e)
        except Exception as e:
            logging.error(e)
        sleep(1)


def init_argparse():
    parser = argparse.ArgumentParser(prog='Search key in files')
    parser.add_argument('keys', type=str, nargs='+', help='Key or keys to search')
    parser.add_argument('-t', '--parallelism', type=int, default=MAX_THREADS, help='Max threads')
    parser.add_argument('-p', '--path', type=str, default='.', help='Path to search')
    return parser


def init_path_list(arg_list, qf: Queue):
    curr_path = arg_list.path
    if not curr_path:
        curr_path = '.'

    logging.debug(f'Search path: {curr_path}')

    for p in glob.iglob(curr_path + '**/**/**/*.*', recursive=True):
        qf.put(Path(p).absolute())

    logging.debug(f'Found {qf.qsize()} files')


def runner_main(argl, qf: Queue, jq: JoinableQueue):
    thread_list = [Thread(target=search_file, args=(argl.keys, qf, jq)) for _ in
                   range(min(argl.parallelism, qf.qsize()))]
    for t in thread_list:
        t.start()

    for t in thread_list:
        t.join()


def join_results(jq: JoinableQueue):
    jq.join()
    res = []
    while not jq.empty():
        res.append(jq.get(False, timeout=1))
    return res


def prepare_tasks(qf: Queue):
    parser = init_argparse()
    argl = parser.parse_args()
    ## use this to override arguments
    # argl.keys = ['test']
    logging.debug(f'Arguments: {argl}')
    init_path_list(argl, qf)
    sleep(1)

    return argl


def finalize(argl, jq: JoinableQueue):
    jq.join()
    res = join_results(jq)

    logging.info(f'Results: {dict({k: res for k in argl.keys})}')


if __name__ == '__main__':
    argl = prepare_tasks(Queue_files)
    runner_main(argl, Queue_files, jQueue_results)

    finalize(argl, jQueue_results)
