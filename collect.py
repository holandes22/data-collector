import shutil


def collect(source, destination):
    shutil.copy(source, destination)


if __name__ == '__main__':
    SRC = '/opt/collector/files.tar.gz'
    DST = '/opt/processor/files.tar.gz'
    collect(SRC, DST)
