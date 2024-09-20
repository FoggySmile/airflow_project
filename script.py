import sys
import pandas as pd
from datetime import datetime, timedelta
import os

if __name__ == "__main__":
    # Получаем дату из аргументов командной строки
    target_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    input_dir = 'input'
    output_dir = 'output'

    # Вычисляем диапазон дат (предыдущие 7 дней)
    start_date = target_date - timedelta(days=7)
    end_date = target_date - timedelta(days=1)

    # Инициализируем пустой DataFrame для накопления данных
    df_total = pd.DataFrame(columns=['email', 'action', 'dt'])

    # Читаем файлы за каждый день в диапазоне
    for i in range(7):
        current_date = start_date + timedelta(days=i)
        file_name = current_date.strftime('%Y-%m-%d') + '.csv'
        file_path = os.path.join(input_dir, file_name)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, names=['email', 'action', 'dt'])
            df_total = pd.concat([df_total, df], ignore_index=True)

    # Группируем данные и считаем количество действий для каждого пользователя
    result = df_total.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()

    # Переименовываем колонки
    result.columns.name = None
    result = result.rename(columns={
        'CREATE': 'create_count',
        'READ': 'read_count',
        'UPDATE': 'update_count',
        'DELETE': 'delete_count'
    })

    # Убедимся, что все столбцы присутствуют
    for action in ['create_count', 'read_count', 'update_count', 'delete_count']:
        if action not in result.columns:
            result[action] = 0

    # Сохраняем результат
    output_file = os.path.join(output_dir, target_date.strftime('%Y-%m-%d') + '.csv')
    result.to_csv(output_file, index=False)
    print(f"Aggregated data saved to {output_file}")
