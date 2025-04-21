import os
import pandas as pd
from glob import glob

def combine_psx_files(input_dir, output_dir):
    # Создаем словарь для группировки файлов по времени
    time_groups = {}
    
    # Ищем все файлы .csv и .txt в директории
    file_patterns = ['psx_*_*.csv', 'psx_*_*.txt']
    for pattern in file_patterns:
        for filepath in glob(os.path.join(input_dir, pattern)):
            filename = os.path.basename(filepath)
            
            # Разбираем имя файла на части
            parts = filename.split('_')
            version = parts[1]
            time_part = '_'.join(parts[2:])
            
            # Удаляем расширение из time_part
            time_key = os.path.splitext(time_part)[0]
            
            # Добавляем файл в соответствующую группу
            if time_key not in time_groups:
                time_groups[time_key] = []
            time_groups[time_key].append((version, filepath))
    
    # Обрабатываем каждую группу файлов
    for time_key, files in time_groups.items():
        dfs = []
        
        for version, filepath in files:
            # Читаем файл в DataFrame
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            elif filepath.endswith('.txt'):
                df = pd.read_csv(filepath, sep='|')  # или другой разделитель
            
            # Добавляем столбец с версией
            df['PSX_Version'] = version
            dfs.append(df)
        
        # Объединяем все DataFrame для этого времени
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Замена отрицательных значений трафика на 0
        combined_df['UpTx'] = combined_df['UpTx'].clip(lower=0)
        combined_df['DownTx'] = combined_df['DownTx'].clip(lower=0)

        # Умножаем значения на 8 для IdPsx от 3 до 5 включительно
        if 'IdPSX' in combined_df.columns:
            mask = (combined_df['IdPSX'] >= 3) & (combined_df['IdPSX'] <= 5)
            combined_df.loc[mask, 'UpTx'] = combined_df.loc[mask, 'UpTx'] * 8
            combined_df.loc[mask, 'DownTx'] = combined_df.loc[mask, 'DownTx'] * 8
            #print(f"Для {len(combined_df[mask])} записей с IdPSX от 3 до 5 значения UpTx и DownTx поделены на 8")
        else:
            print("Предупреждение: столбец IdPSX не найден, деление не выполнено")

        # Создаем имя выходного файла
        output_filename = f"psx_{time_key}.csv"
        output_path = os.path.join(output_dir, output_filename)
        
        # Сохраняем объединенный DataFrame
        combined_df.to_csv(output_path, index=False)
        print(f"Создан файл: {output_path}")

# Пример использования
input_directory = ''
output_directory = 'splitted'

# Создаем выходную директорию, если она не существует
os.makedirs(output_directory, exist_ok=True)

combine_psx_files(input_directory, output_directory)