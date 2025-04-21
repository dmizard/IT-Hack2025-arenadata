import pandas as pd
import glob
import os

def main():
    # Загрузка данных
    try:
        # Загрузка anomaly_report.csv
        anomaly_report = pd.read_csv('anomaly_report.csv')
        anomaly_report['Traffic'] = anomaly_report['UpTx'] - anomaly_report['DownTx']
        
        # Загрузка subscribers.csv
        subscribers = pd.read_csv('subscribers.csv')
        
        # Загрузка company.parquet
        company_df = pd.read_parquet('company.parquet')
        company_df['Type'] = 'C'  # Добавляем тип для компаний
        
        # Загрузка physical.parquet
        physical_df = pd.read_parquet('physical.parquet')
        physical_df['Type'] = 'P'  # Добавляем тип для физлиц
        
        # Объединение данных о клиентах
        clients_df = pd.concat([company_df, physical_df], ignore_index=True)
        
        # Соединение anomaly_report с subscribers (IdSubscriber = IdOnPSX)
        merged = anomaly_report.merge(
            subscribers, 
            left_on='IdSubscriber', 
            right_on='IdOnPSX',
            how='left'
        )

        # Соединение с данными о клиентах (IdClient = Id)
        final_df = merged.merge(
            clients_df,
            left_on='IdClient',
            right_on='Id',
            how='left'
        )
        
        # Выбор нужных колонок и переименование
        result = final_df[[
            'IdSubscriber', 'IdClient', 'Type', 'IdPlan', 'Status', 'Traffic'
        ]].rename(columns={
            'IdSubscriber': 'Id',
            'IdClient': 'UID',
            'Status': 'TurnOn'
        })
        result['TurnOn'] = result['TurnOn'].replace({'ON': True, 'OFF': False})
        result.insert(result.columns.get_loc('TurnOn') + 1, 'Hacked', True)
        
        # Сохранение результата
        result.to_csv('result.csv', index=False)
        print("Отчет успешно сохранен в result.csv")
        
        # Вывод первых строк для проверки
        print("\nПервые строки отчета:")
        print(result.head().to_string(index=False))
        
    except Exception as e:
        print(f"Ошибка при обработке данных: {str(e)}")

if __name__ == "__main__":
    main()