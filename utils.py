from os import listdir
from os.path import isfile, join, split
import Queue
import threading
import json
import codecs
import requests


# list files in a folder and put them in to a queue for multi-threading processing
def multi_thread_process_files(dir_path, file_extension, num_threads, process_func,
                               proc_desc='processed', args=None, multi=None,
                               file_filter_func=None, callback_func=None,
                               thread_wise_objs=None):
    onlyfiles = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    num_pdfs = 0
    files = None if multi is None else []
    lst = []
    for f in onlyfiles:
        if f.endswith('.' + file_extension) if file_filter_func is None \
                else file_filter_func(f):
            if multi is None:
                lst.append(join(dir_path, f))
            else:
                files.append(join(dir_path, f))
                if len(files) >= multi:
                    lst.append(files)
                    files = []
            num_pdfs += 1
    if files is not None and len(files) > 0:
        lst.append(files)
    multi_thread_tasking(lst, num_threads, process_func, proc_desc, args, multi, file_filter_func,
                         callback_func,
                         thread_wise_objs=thread_wise_objs)


def multi_thread_tasking(lst, num_threads, process_func,
                               proc_desc='processed', args=None, multi=None,
                               file_filter_func=None, callback_func=None, thread_wise_objs=None):
    num_pdfs = len(lst)
    pdf_queque = Queue.Queue(num_pdfs)
    # print('putting list into queue...')
    for item in lst:
        pdf_queque.put_nowait(item)
    thread_num = min(num_pdfs, num_threads)
    arr = [process_func] if args is None else [process_func] + args
    arr.insert(0, pdf_queque)
    # print('queue filled, threading...')
    for i in range(thread_num):
        tarr = arr[:]
        thread_obj = None
        if thread_wise_objs is not None and isinstance(thread_wise_objs, list):
            thread_obj = thread_wise_objs[i]
        tarr.insert(0, thread_obj)
        t = threading.Thread(target=multi_thread_do, args=tuple(tarr))
        t.daemon = True
        t.start()

    # print('waiting jobs to finish')
    pdf_queque.join()
    # print('{0} files {1}'.format(num_pdfs, proc_desc))
    if callback_func is not None:
        callback_func(*tuple(args))


def multi_thread_do(thread_obj, q, func, *args):
    while True:
        p = q.get()
        try:
            if thread_obj is not None:
                func(thread_obj, p, *args)
            else:
                func(p, *args)
        except Exception, e:
            print u'error doing {0} on {1} \n{2}'.format(func, p, str(e))
        q.task_done()


def save_json_array(lst, file_path):
    with codecs.open(file_path, 'w', encoding='utf-8') as wf:
        json.dump(lst, wf, encoding='utf-8')


def load_json_data(file_path):
    data = None
    with codecs.open(file_path, encoding='utf-8') as rf:
        data = json.load(rf, encoding='utf-8')
    return data


def http_post_result(url, payload, headers=None, auth=None):
    req = requests.post(
        url, headers=headers,
        data=payload, auth=auth)
    return req.content.decode("utf-8")

def main():
    pass

if __name__ == "__main__":
    main()
