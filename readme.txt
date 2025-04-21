Для того чтобы скрипты успешно работали для начала нужно сбросить их все в папку с датасетами, то есть в telecom1000k

Должны быть установленные библиотеки:
pip install pandas datetime glob os multiprocessing functools traceback csv collections
Для запуска программы нужно ввести команды соответственно:

python splitter.py
python anomalycheck.py
python finaler.py

Для того, чтобы посмотреть дубликаты можно командой
python dublicatecheck.py
ВАЖНО! Это возможно только после создания директории splitted, которая создается после завершения работы скрипта splitter.py