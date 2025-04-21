import os
import csv
from collections import defaultdict

def find_duplicates_in_single_file(filepath):
    """Поиск дубликатов в одном файле"""
    subscribers = defaultdict(list)
    
    with open(filepath, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок
        
        for row in reader:
            if len(row) >= 4:  # Проверяем, что в строке достаточно данных
                id_session = row[0].strip()
                id_subscriber = row[2].strip()
                subscribers[id_subscriber].append(id_session)
    
    # Возвращаем только настоящие дубликаты (где один подписчик имеет несколько сессий)
    return {sub: sessions for sub, sessions in subscribers.items() if len(sessions) > 1}

def process_files():
    # Получаем путь к директории скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, 'duplicates_report.txt')
    
    # Открываем файл для записи отчета
    with open(output_file, 'w', encoding='utf-8') as report:
        report.write("ОТЧЕТ О ДУБЛИКАТАХ ПО ФАЙЛАМ\n")
        report.write("="*50 + "\n\n")
        
        # Обрабатываем каждый файл в директории
        for filename in os.listdir(script_dir):
            if filename.startswith('psx_') and filename.endswith('.csv'):
                filepath = os.path.join(script_dir, filename)
                
                try:
                    duplicates = find_duplicates_in_single_file(filepath)
                    
                    if duplicates:
                        print(f"\nНайдены дубликаты в файле {filename}:")
                        report.write(f"Файл: {filename}\n")
                        report.write("-"*45 + "\n")
                        
                        for sub, sessions in duplicates.items():
                            print(f"  Подписчик {sub} имеет {len(sessions)} сессий: {', '.join(sessions)}")
                            report.write(f"Подписчик {sub} имеет {len(sessions)} сессий:\n")
                            report.write(f"ID сессий: {', '.join(sessions)}\n\n")
                        
                        report.write("\n")
                    else:
                        print(f"В файле {filename} дубликатов не найдено.")
                        
                except Exception as e:
                    print(f"Ошибка при обработке файла {filename}: {str(e)}")
                    report.write(f"Ошибка в файле {filename}: {str(e)}\n\n")
                    continue
    
    print(f"\nПолный отчет сохранен в файл: {output_file}")

if __name__ == "__main__":
    process_files()