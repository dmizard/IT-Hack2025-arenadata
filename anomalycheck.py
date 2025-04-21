import pandas as pd
from datetime import datetime
import glob
import os
from multiprocessing import Pool, cpu_count, current_process, Manager, Lock
from functools import partial
import traceback

def log(message):
    pid = current_process().pid
    print(f"[PID {pid}] {message}", flush=True)

def parse_date(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str) or date_str.strip() == '':
        return pd.NaT
    
    date_formats = [
        '%d-%m-%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S', 
        '%m/%d/%Y %H:%M:%S',
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    return pd.NaT

def analyze_single_file(file_path, lock, report_file, latest_sessions):
    """Обработка одного файла с записью в общий отчет"""
    try:
        # Чтение данных
        df = pd.read_csv(
            file_path,
            dtype={'StartSession': str, 'EndSession': str},
            on_bad_lines='warn'
        )
        
        # Преобразование дат
        df['StartSession'] = df['StartSession'].apply(parse_date)
        df['EndSession'] = df['EndSession'].apply(parse_date)
        df = df[df['StartSession'].notna()].copy()
        
        if df.empty:
            log(f"Файл {file_path} пуст, пропускаю")
            return 0
        
        # Вычисление аномалий трафика
        df['UpTx_DownTx_Ratio'] = df['UpTx'] - df['DownTx']
        abnormal_traffic = df[df['UpTx_DownTx_Ratio'] > 0].copy()
        
        if abnormal_traffic.empty:
            log(f"В файле {file_path} аномалий не найдено")
            return 0
        
        # Обновление информации о последних сессиях
        with lock:
            for _, row in abnormal_traffic.iterrows():
                sub_id = row['IdSubscriber']
                session_end = row['EndSession'] if pd.notna(row['EndSession']) else datetime.max
                
                # Если сессия более новая или еще не было данных по этому абоненту
                if sub_id not in latest_sessions or session_end > latest_sessions[sub_id]['EndSession']:
                    latest_sessions[sub_id] = {
                        'IdSession': row['IdSession'],
                        'UpTx': row['UpTx'],
                        'DownTx': row['DownTx'],
                        'UpTx_DownTx_Ratio': row['UpTx_DownTx_Ratio'],
                        'EndSession': session_end
                    }
        
        return 1
        
    except Exception as e:
        log(f"ОШИБКА в файле {file_path}: {str(e)}\n{traceback.format_exc()}")
        return 0

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    splitted_dir = os.path.join(base_dir, 'splitted')
    report_file = os.path.join(base_dir, 'anomaly_report.csv')
    
    # Очистка предыдущего отчета
    if os.path.exists(report_file):
        os.remove(report_file)
    
    # Получаем список файлов
    psx_files = glob.glob(os.path.join(splitted_dir, 'psx_*.csv'))
    
    if not psx_files:
        print(f"Нет файлов в {splitted_dir}!")
        return
    
    print(f"Найдено {len(psx_files)} файлов для обработки")
    
    # Создаем менеджер для межпроцессного взаимодействия
    with Manager() as manager:
        lock = manager.Lock()
        latest_sessions = manager.dict()  # Хранит последние сессии по абонентам
        
        # Настраиваем параллельную обработку
        num_workers = max(1, cpu_count() - 1)
        analyze_func = partial(analyze_single_file,
                             lock=lock,
                             report_file=report_file,
                             latest_sessions=latest_sessions)
        
        # Запускаем обработку
        with Pool(num_workers) as pool:
            results = pool.map(analyze_func, psx_files)
        
        # Выводим итоги
        files_with_anomalies = sum(results)
        print("\nИтоги обработки:")
        print(f"Всего файлов: {len(psx_files)}")
        print(f"Файлов с аномалиями: {files_with_anomalies}")
        print(f"Файлов без аномалий: {len(psx_files) - files_with_anomalies}")
        
        if files_with_anomalies > 0:
            # Создаем DataFrame из последних сессий
            final_report = pd.DataFrame.from_dict(dict(latest_sessions), orient='index')
            final_report.reset_index(inplace=True)
            final_report.rename(columns={'index': 'IdSubscriber'}, inplace=True)
            
            # Сохраняем отчет
            final_report[[
                'IdSubscriber', 'IdSession', 'UpTx', 'DownTx', 'UpTx_DownTx_Ratio'
            ]].to_csv(report_file, index=False, float_format='%.15g')
            
            print(f"\nОтчет сохранен в: {report_file}")
            print(f"Уникальных абонентов с аномалиями: {len(final_report)}")
            
            # Выводим пример данных для проверки формата
            print("\nПример данных из отчета:")
            print(final_report.head().to_string(index=False))

if __name__ == "__main__":
    main()