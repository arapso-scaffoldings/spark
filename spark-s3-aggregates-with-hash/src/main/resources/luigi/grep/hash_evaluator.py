import hashlib
import tempfile
from boto.exception import S3ResponseError
from luigi.contrib.s3 import S3Client
from luigi import Task


def gen_hash(files):
    a = hashlib.md5()
    for file in files:
        a.update(file)
    return a.hexdigest()


def filter_files(files, file_name_filter):
    temp = list()
    for file in files:
        if file_name_filter in file:
            temp.append(file)
    return sorted(temp)


def get_hash_s3_files(input_path, file_name_filter):
    s3 = S3Client()
    files = s3.listdir(input_path)
    return gen_hash(filter_files(files, file_name_filter))


def get_old_hash(path):
    try:
        s3 = S3Client()
        hash_local_file = tempfile.mktemp()
        s3.get(path, hash_local_file)
        with open(hash_local_file, 'r') as f:
            return f.readline().strip()
    except S3ResponseError:
        return None


def put_new_hash(path, hash_value):
    hash_local_file = tempfile.mktemp()
    with open(hash_local_file, 'w') as f:
        f.write(hash_value + "\n")
    s3 = S3Client()
    s3.put(hash_local_file, path)


def remove_old_files(path, recursive=True):
    s3_client = S3Client()
    s3_client.remove(path, recursive=recursive)


def is_aggregates_valid(that, src_files_path, src_files_filter, aggregates_path, hash_path):
    files_uploaded = Task.complete(that)
    if not files_uploaded:
        return False
    eval_hash = get_hash_s3_files(src_files_path, src_files_filter)

    old_hash = get_old_hash(hash_path)
    if old_hash is None:
        put_new_hash(hash_path, eval_hash)
        return True

    print "Evaluated hash: '%s' - Saved hash: '%s'" % (eval_hash, old_hash)
    if old_hash == eval_hash:
        print "Hashes are equal."
        return True

    print("Files from {} will be removed.".format(aggregates_path))
    remove_old_files(aggregates_path)
    remove_old_files(hash_path, False)
    return False