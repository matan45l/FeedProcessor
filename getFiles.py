import boto  # Imports credentials from local env
import gzip
import StringIO


def getFileObject(filename):
    GOOGLE_STORAGE = 'gs'

    src_uri = boto.storage_uri('stuff-bucket' + '/' + filename, GOOGLE_STORAGE)
    object_contents = StringIO.StringIO()
    src_uri.get_key().get_file(object_contents)
    object_contents.seek(0)

    if 'gz' in filename.split('.')[-1] :
        object_contents = gzip.open(fileobj=object_contents)

    return object_contents


