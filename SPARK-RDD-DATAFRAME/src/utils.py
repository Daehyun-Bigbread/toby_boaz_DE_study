import os

def raw_file_path(target_date: str):
    return os.path.join("..", "data", "raw", f"{target_date}.csv")

def processed_path(target_date: str):
    return os.path.join("..", "data", "processed", target_date)

def processed_glob_path():
    return os.path.join("..", "data", "processed", "*")

def datamart_file_path(file_name: str):
    return os.path.join("..", "data", "datamart", file_name)