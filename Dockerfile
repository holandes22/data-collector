FROM python:3.5-onbuild

RUN useradd -d /home/user -m -s /bin/bash user

CMD [ "pyinstaller", "collect.spec" ]
